"""Use installed SDK primitives without a server, UI, state directory or network."""

import json
from dataclasses import replace

from aethermesh_core import Job, LocalRunner, NodeIdentity, validate_job_result

job = Job(job_id="sdk-echo", job_type="echo", payload={"message": "hello mesh"})
result = LocalRunner(NodeIdentity(node_id="sdk-example")).run(job)
validation = validate_job_result(job, result)
if (
    result.status != "completed"
    or result.output != "hello mesh"
    or not validation.valid
):
    raise RuntimeError("local SDK example failed")

# A receipt/result must be checked, not trusted just because it was returned.
tampered = replace(result, output="unexpected content")
if validate_job_result(job, tampered).valid:
    raise RuntimeError("tampered local result was accepted")

print(
    json.dumps(
        {"mode": "local-only-no-p2p", "result": result.to_dict()}, sort_keys=True
    )
)
