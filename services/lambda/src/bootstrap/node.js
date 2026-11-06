// devcloud Lambda bootstrap for Node.js runtimes (embedded by runtime.rs).
//
// One process is one execution environment: the handler module is loaded
// once (the init phase), then invocations arrive one JSON line at a time on
// fd 3 (stdin is the handler's, and empty). Each outcome goes to the request's result file, and a
// "<marker> <tag>" line on stdout tells devcloud where the invocation ends
// ("ready" after init, "done" after an invocation, "done reset" when the
// environment must not be reused).
const fs = require('fs');
const path = require('path');
const url = require('url');

const markerPrefix = process.env._DEVCLOUD_MARKER;
const functionArn = process.env._DEVCLOUD_FUNCTION_ARN;
const expectedVersion = process.env._DEVCLOUD_RUNTIME_VERSION || '';
for (const k of ['_DEVCLOUD_MARKER', '_DEVCLOUD_FUNCTION_ARN', '_DEVCLOUD_RUNTIME_VERSION']) delete process.env[k];
process.stderr.write = process.stdout.write.bind(process.stdout);
if (expectedVersion && expectedVersion !== process.versions.node.split('.')[0]) {
  console.log(`[WARNING] devcloud: runtime ${process.env.AWS_EXECUTION_ENV.slice('AWS_Lambda_'.length)} is running on Node.js ${process.versions.node} (${process.execPath})`);
}

// Pipe writes are asynchronous: `then` runs once everything queued on stdout
// (stderr is routed there too) has been handed to the OS, so the marker never
// overtakes the invocation's own output.
const marker = (tag, then) => process.stdout.write(`${markerPrefix} ${tag}\n`, then);

const errorBody = (e, type) => ({
  errorType: type || (e && e.name) || 'Error',
  errorMessage: e && e.message !== undefined ? String(e.message) : String(e),
  trace: e && e.stack ? String(e.stack).split('\n') : [],
});

// The next request, read synchronously: between invocations the environment
// is frozen (timers and I/O left behind resume with the next invocation), as
// on AWS. `null` once devcloud closes the control pipe.
const CONTROL_FD = 3;
const chunks = [];
const readRequest = () => {
  const sleeper = new Int32Array(new SharedArrayBuffer(4));
  for (;;) {
    const buf = Buffer.alloc(64 * 1024);
    let n;
    try {
      n = fs.readSync(CONTROL_FD, buf, 0, buf.length, null);
    } catch (e) {
      if (e.code === 'EAGAIN') { Atomics.wait(sleeper, 0, 0, 5); continue; }
      if (e.code === 'EOF') return null;
      throw e;
    }
    if (n === 0) return null;
    const chunk = buf.subarray(0, n);
    const nl = chunk.indexOf(10);
    if (nl < 0) { chunks.push(chunk); continue; }
    chunks.push(chunk.subarray(0, nl));
    const line = Buffer.concat(chunks).toString('utf8');
    chunks.length = 0;
    if (nl + 1 < n) chunks.push(chunk.subarray(nl + 1));
    return JSON.parse(line);
  }
};

// Node's ESM loader ignores NODE_PATH, which is how layer dependencies under
// /opt are found. As in the Lambda Node runtime, ES module handlers resolve
// them too: a resolve hook retries a bare specifier the default resolution
// could not find against each NODE_PATH directory.
const ESM_LAYER_HOOK = `
import { createRequire } from 'node:module';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
let dirs = [];
export function initialize(data) { dirs = data.dirs; }
export async function resolve(specifier, context, next) {
  try {
    return await next(specifier, context);
  } catch (e) {
    const bare = !/^([./]|[a-zA-Z][a-zA-Z0-9+.-]*:)/.test(specifier);
    if (!bare || !e || e.code !== 'ERR_MODULE_NOT_FOUND') throw e;
    for (const dir of dirs) {
      // ESM resolution looks in <ancestor>/node_modules of the parent.
      if (path.basename(dir) === 'node_modules') {
        try {
          return await next(specifier, { ...context, parentURL: pathToFileURL(path.join(path.dirname(dir), 'index.mjs')).href });
        } catch {}
      }
      try {
        // CommonJS resolution honours NODE_PATH as is (any directory name).
        const file = createRequire(path.join(dir, 'index.js')).resolve(specifier);
        return { url: pathToFileURL(file).href, shortCircuit: true };
      } catch {}
    }
    throw e;
  }
}
`;
// `.mjs`, or `.js` under a package.json with "type": "module" (the nearest
// package.json decides, as in Node).
const isEsm = (file) => {
  if (file.endsWith('.mjs')) return true;
  if (!file.endsWith('.js')) return false;
  for (let dir = path.dirname(file); ; dir = path.dirname(dir)) {
    const pkg = path.join(dir, 'package.json');
    if (fs.existsSync(pkg)) {
      try {
        return JSON.parse(fs.readFileSync(pkg, 'utf8')).type === 'module';
      } catch {
        return false;
      }
    }
    if (path.dirname(dir) === dir) return false;
  }
};

let esmHookRegistered = false;
const importEsm = (file) => {
  const dirs = (process.env.NODE_PATH || '').split(path.delimiter).filter(Boolean);
  const { register } = require('module');
  if (!esmHookRegistered && dirs.length && typeof register === 'function') {
    register(`data:text/javascript,${encodeURIComponent(ESM_LAYER_HOOK)}`, { data: { dirs } });
    esmHookRegistered = true;
  }
  return import(url.pathToFileURL(file).href);
};

let fn;
let initError;

const loadHandler = async () => {
  const root = process.env.LAMBDA_TASK_ROOT;
  // As in the AWS Node runtime: `dir/module.prop.path` — the module is the
  // basename up to its *first* dot, the rest is a property path into the
  // exports (`index.api.handler` → module `index`, `exports.api.handler`).
  const spec = process.env._HANDLER;
  const slash = spec.lastIndexOf('/');
  const dot = spec.indexOf('.', slash + 1);
  if (dot <= slash + 1 || dot === spec.length - 1) {
    initError = { errorType: 'Runtime.MalformedHandlerName', errorMessage: `Bad handler ${spec}`, trace: [] };
    return;
  }
  const modName = spec.slice(0, dot);
  const fnName = spec.slice(dot + 1);
  const fnPath = fnName.split('.');
  let mod;
  try {
    // Like the AWS Node runtime: `<name>.js|.mjs|.cjs` first, then Node's own
    // resolution (directory index.js, package.json "main"/"exports", ...).
    const base = path.resolve(root, modName);
    const file = ['.js', '.mjs', '.cjs'].map((ext) => base + ext).find((f) => fs.existsSync(f))
      || require.resolve(base);
    if (isEsm(file)) {
      // Through import(), never require(): Node 22's require() can load ESM
      // too, but without the layer resolve hook.
      mod = await importEsm(file);
    } else {
      try {
        mod = require(file);
      } catch (e) {
        // ESM that require() cannot load synchronously (Node 22 can require
        // some ESM, but not modules with top-level await).
        if (e && (e.code === 'ERR_REQUIRE_ESM' || e.code === 'ERR_REQUIRE_ASYNC_MODULE')) {
          mod = await importEsm(file);
        } else throw e;
      }
    }
  } catch (e) {
    initError = errorBody(e, 'Runtime.ImportModuleError');
    return;
  }
  const walk = (obj) => fnPath.reduce((o, key) => (o == null ? undefined : o[key]), obj);
  fn = walk(mod) !== undefined || mod == null ? walk(mod) : walk(mod.default);
  if (typeof fn !== 'function') {
    initError = { errorType: 'Runtime.HandlerNotFound', errorMessage: `${modName}.${fnName} is undefined or not exported`, trace: [] };
  }
};

// The invocation in flight, so an error that escapes the handler's own
// promise chain still fails it, as in the AWS Node runtime.
let current = null;
const fatal = (errorType) => (e) => {
  console.error(`ERROR\t${errorType === 'Runtime.UnhandledPromiseRejection' ? 'Unhandled Promise Rejection' : 'Uncaught Exception'}\t${e && e.stack ? e.stack : e}`);
  if (current) {
    const body = errorType === 'Runtime.UnhandledPromiseRejection'
      ? { errorType, errorMessage: e && e.name ? `${e.name}: ${e.message}` : String(e), trace: e && e.stack ? String(e.stack).split('\n') : [] }
      : errorBody(e);
    try { fs.writeFileSync(current.resultPath, JSON.stringify({ error: body })); } catch {}
    current = null;
    // The environment is in an unknown state: it is discarded.
    marker('done reset', () => process.exit(1));
    return;
  }
  // Outside an invocation (init, or a callback-free wait): the runtime
  // exits, as Node itself would.
  process.stdout.write('', () => process.exit(1));
};
process.on('unhandledRejection', fatal('Runtime.UnhandledPromiseRejection'));
process.on('uncaughtException', fatal());

const invoke = async (req) => {
  current = req;
  const write = (obj) => { if (current === req) fs.writeFileSync(req.resultPath, JSON.stringify(obj)); };
  const finish = (obj) => {
    if (current !== req) return;
    write(obj);
    current = null;
    marker('done', next);
  };
  if (initError) {
    // Like Lambda: a failed init fails the invocation and the environment is
    // discarded.
    write({ error: initError });
    marker('done reset', () => process.exit(0));
    return;
  }
  const event = JSON.parse(req.event || '{}');
  const context = {
    functionName: process.env.AWS_LAMBDA_FUNCTION_NAME,
    functionVersion: process.env.AWS_LAMBDA_FUNCTION_VERSION,
    invokedFunctionArn: functionArn,
    memoryLimitInMB: process.env.AWS_LAMBDA_FUNCTION_MEMORY_SIZE,
    awsRequestId: req.requestId,
    logGroupName: process.env.AWS_LAMBDA_LOG_GROUP_NAME,
    logStreamName: process.env.AWS_LAMBDA_LOG_STREAM_NAME,
    callbackWaitsForEmptyEventLoop: true,
    getRemainingTimeInMillis: () => Math.max(0, req.deadlineMs - Date.now()),
  };
  let result;
  let failure;
  // Separate flag: `Promise.reject()` / `throw undefined` fail with an
  // undefined reason, which must still be reported as a function error.
  let failed = false;
  let settledByCallback = false;
  let drained;
  try {
    // Same completion rules as the AWS Node runtime: a returned promise
    // settles the invocation; otherwise wait for the callback, however the
    // handler declares it (default value, rest args, `arguments`). If the
    // event loop drains without a callback, the result is null. `fn.length`
    // says nothing about whether a callback is coming.
    result = await new Promise((resolve, reject) => {
      const callback = (err, res) => {
        settledByCallback = true;
        return err ? reject(err) : resolve(res);
      };
      // No callback by the time the loop drains → null. Deferred one tick so
      // a handler that itself calls back from a `beforeExit` listener (in
      // the same emission) still wins.
      drained = () => setImmediate(() => resolve(undefined));
      process.once('beforeExit', drained);
      const ret = fn(event, context, callback);
      if (ret && typeof ret.then === 'function') ret.then(resolve, reject);
    });
  } catch (e) {
    failed = true;
    failure = e;
  }
  process.removeListener('beforeExit', drained);
  if (failed) {
    console.error(`ERROR\tInvoke Error\t${failure && failure.stack ? failure.stack : failure}`);
    return finish({ error: errorBody(failure) });
  }
  let body;
  try {
    body = JSON.stringify(result === undefined ? null : result);
  } catch (e) {
    return finish({ error: errorBody(e, 'Runtime.MarshalError') });
  }
  // A successful callback response honours callbackWaitsForEmptyEventLoop
  // (default true): record the result now and report the invocation done
  // once timers and I/O the handler left running have finished (devcloud
  // bounds the wait by the function timeout). If the loop has already
  // drained — e.g. the handler called back from a `beforeExit` listener — no
  // further `beforeExit` comes and the process ends instead; devcloud then
  // takes the recorded result from the exited environment. Errors, promise
  // results, and handlers that set the flag to false finish immediately.
  if (settledByCallback && context.callbackWaitsForEmptyEventLoop) {
    write({ result: body });
    process.once('beforeExit', () => { if (current === req) { current = null; marker('done', next); } });
    return;
  }
  finish({ result: body });
};

function next() {
  const req = readRequest();
  if (!req) process.exit(0);
  invoke(req);
}

(async () => {
  await loadHandler();
  marker('ready', next);
})();
