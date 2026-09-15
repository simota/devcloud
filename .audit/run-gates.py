"""Discover and execute every gate/E2E, retaining each real exit status."""
import json
import os
import pathlib
import re
import subprocess
import sys
import time

group = sys.argv[1]
result_dir = pathlib.Path(os.environ['RUNNER_TEMP']) / ('audit-' + group)
result_dir.mkdir(parents=True, exist_ok=True)
services = {'s3', 'gcs', 'bigquery', 'pubsub', 'dynamodb', 'redshift', 'sqs', 'mail', 'redis', 'applicationautoscaling'}
results = []
for path in sorted(pathlib.Path('scripts').glob('*/verify.sh')) + sorted(pathlib.Path('scripts').glob('*-e2e.sh')):
    component = (path.parent.name if path.name == 'verify.sh' else path.name).split('-')[0]
    category = component if component in services else 'misc'
    if category != group:
        continue
    env = os.environ.copy()
    stage = None
    if path.name == 'verify.sh':
        text = path.read_text()
        cases = list(re.finditer(r'case .*VERIFY_STAGE.* in', text))
        if cases:
            labels = re.findall(r'^  ([a-zA-Z0-9_|-]+)\)', text[cases[-1].end():], re.M)
            finals = [x for x in labels if 'full' in x]
            if not finals:
                raise RuntimeError('No verified final stage for ' + str(path))
            stage = finals[-1].split('|')[-1]
            env['VERIFY_STAGE'] = stage
    name = str(path).replace('/', '__')
    log = result_dir / (name + '.log')
    start = time.monotonic()
    with log.open('w') as output:
        try:
            proc = subprocess.run(['bash', str(path)], env=env, stdout=output, stderr=subprocess.STDOUT, timeout=900)
            code = proc.returncode
        except subprocess.TimeoutExpired:
            code = 124
    text = log.read_text(errors='replace')
    nested_failure = bool(re.search(r'(?m)^\[FAIL\]|test result: FAILED|error: could not compile', text))
    status = 'PASS' if code == 0 and not nested_failure else 'FAIL'
    item = {'path': str(path), 'stage': stage, 'exit': code, 'nested_failure': nested_failure, 'status': status, 'seconds': round(time.monotonic()-start, 2)}
    results.append(item)
    (result_dir / 'outcomes.json').write_text(json.dumps(results, indent=2))
    print(status, str(path), stage or '', 'exit=' + str(code), flush=True)
    if status != 'PASS':
        print('\n'.join(text.splitlines()[-35:]), flush=True)
if not results:
    raise RuntimeError('No gates discovered for ' + group)
sys.exit(0 if all(x['status'] == 'PASS' for x in results) else 1)
