# devcloud Lambda bootstrap for Python runtimes (embedded by runtime.rs).
#
# One process is one execution environment: the handler module is imported
# once (the init phase), then invocations arrive one JSON line at a time on
# fd 3 (stdin is the handler's, and empty). Each outcome goes to the request's result file, and a
# "<marker> <tag>" line on stdout tells devcloud where the invocation ends
# ("ready" after init, "done" after an invocation, "done reset" when the
# environment must not be reused).
import importlib, json, os, sys, time, traceback

def _devcloud_main():
    marker_prefix = os.environ.pop("_DEVCLOUD_MARKER")
    function_arn = os.environ.pop("_DEVCLOUD_FUNCTION_ARN")
    expected_version = os.environ.pop("_DEVCLOUD_RUNTIME_VERSION", "")
    # Requests come in on fd 3, a pipe of their own: the handler cannot
    # swallow them through stdin.
    control = os.fdopen(3, "rb")
    real_stdout = sys.stdout
    sys.stderr = sys.stdout

    def marker(tag):
        real_stdout.flush()
        real_stdout.write("%s %s\n" % (marker_prefix, tag))
        real_stdout.flush()

    actual_version = "%d.%d" % sys.version_info[:2]
    if expected_version and expected_version != actual_version:
        print("[WARNING] devcloud: runtime %s is running on Python %s (%s)" % (os.environ["AWS_EXECUTION_ENV"][len("AWS_Lambda_"):], sys.version.split()[0], sys.executable))

    class LambdaContext:
        function_name = os.environ["AWS_LAMBDA_FUNCTION_NAME"]
        function_version = os.environ["AWS_LAMBDA_FUNCTION_VERSION"]
        invoked_function_arn = function_arn
        memory_limit_in_mb = os.environ["AWS_LAMBDA_FUNCTION_MEMORY_SIZE"]
        log_group_name = os.environ["AWS_LAMBDA_LOG_GROUP_NAME"]
        log_stream_name = os.environ["AWS_LAMBDA_LOG_STREAM_NAME"]
        identity = None
        client_context = None

        def __init__(self, request_id, deadline_ms):
            self.aws_request_id = request_id
            self._deadline_ms = deadline_ms

        def get_remaining_time_in_millis(self):
            return max(0, self._deadline_ms - int(time.time() * 1000))

    # ---- init ----
    init_error = None
    handler = None
    root = os.environ["LAMBDA_TASK_ROOT"]
    sys.path.insert(0, root)
    handler_spec = os.environ["_HANDLER"]
    module_name, _, func_name = handler_spec.rpartition(".")
    if not module_name:
        init_error = ("Bad handler '%s': not enough values to unpack (expected 2, got 1)" % handler_spec, "Runtime.MalformedHandlerName")
    else:
        try:
            module = importlib.import_module(module_name.replace("/", "."))
        except Exception as e:
            init_error = ("Unable to import module '%s': %s" % (module_name, e), "Runtime.ImportModuleError")
        else:
            handler = getattr(module, func_name, None)
            if handler is None:
                init_error = ("Handler '%s' missing on module '%s'" % (func_name, module_name), "Runtime.HandlerNotFound")
    marker("ready")

    # ---- invocations ----
    while True:
        line = control.readline()
        if not line:
            return
        req = json.loads(line)
        request_id = req["requestId"]

        def write(obj, path=req["resultPath"]):
            with open(path, "w") as f:
                json.dump(obj, f)

        def failure(message, error_type, stack):
            write({"error": {"errorMessage": message, "errorType": error_type, "requestId": request_id, "stackTrace": stack}})

        if init_error is not None:
            # Like Lambda: a failed init fails the invocation and the
            # environment is discarded.
            failure(init_error[0], init_error[1], [])
            marker("done reset")
            return
        event = json.loads(req["event"] or "{}")
        try:
            result = handler(event, LambdaContext(request_id, req["deadlineMs"]))
        except Exception as e:
            frames = traceback.extract_tb(e.__traceback__)[1:]
            print("[ERROR] %s: %s" % (type(e).__name__, e))
            failure(str(e), type(e).__name__, traceback.format_list(frames))
            marker("done")
            continue
        try:
            body = json.dumps(result)
        except Exception as e:
            failure("Unable to marshal response: %s" % e, "Runtime.MarshalError", [])
            marker("done")
            continue
        write({"result": body})
        marker("done")

try:
    _devcloud_main()
except BaseException:
    traceback.print_exc()
finally:
    # End now: a normal interpreter exit would wait for non-daemon threads the
    # handler left behind.
    sys.stdout.flush()
    sys.__stdout__.flush()
    sys.__stderr__.flush()
    os._exit(0)
