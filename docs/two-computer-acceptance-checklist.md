# Two-computer acceptance checklist

Status: **not run on two computers**. This worksheet prepares the existing
[two-peer runbook](two-peer-operations.md); it does not authorize installation,
credential creation, network changes or listeners. Current automated evidence is
installed packages in isolated local/loopback fixtures.

## Smallest input set

Complete these six rows before any real-machine run. Keep private paths and
topology in the operators' local record; never put private keys in shared logs.

| Input | Operator record |
| --- | --- |
| Machines | A label/owner/OS/Python; B label/owner/OS/Python; confirmation these are distinct computers with approved operator access. |
| Installation | Approval for separate application-owned environments; the same reviewed SDK wheel hash; exact approved dependency artifacts for each target Python/OS. No active Eidolon checkout changes. |
| Existing identities | A/server and B/client certificate/key/CA file locations, appropriate role usages and validity dates, independently verified public leaf pins, and the owner responsible for rotation. Keys stay on their owning machines. |
| Endpoint and permission | A's approved private interface and TCP port, B's reachable address for A, A's certificate SAN server name, and explicit permission for this foreground listener and client connection. If routing/firewall/VPN changes are needed, stop for separate approval. |
| Project and bytes | One selected project and allowed reader; approved nonsecret fixture at most 8 MiB; independently selected manifest and whole-file digests. For a bundle, also the trusted source branch and full commit OID. |
| Local storage and budget | Owned physical checkpoint/output directories outside active checkouts, absent output path, supported POSIX filesystem, finite per-invocation timeout, explicit lifetime server byte budget and cleanup owner. |

If identities do not already exist, issuance requires a separate specific
decision; test fixture identities are not deployment credentials. This worksheet
does not create or enroll them. A reachable approved LAN/VPN endpoint is enough
for direct connectivity; automatic discovery, NAT traversal and relay service
are not supplied. Windows transfer targets are not currently validated.

## Run and capture

### Offline fixture and worksheet preparation

The repository's `scripts/prepare_peer_acceptance.py` generates only local public
bytes and a blank evidence worksheet. It does not connect, listen, create/load
credentials, enroll trust or install anything. Run it using an existing approved
environment containing the SDK, against a new directory in a trusted, stable
physical parent (for example `/private/tmp` rather than its `/tmp` symlink on
macOS). No existing directory is reused:

```sh
APPROVED_PYTHON /physical/path/to/reviewed/sdk/scripts/prepare_peer_acceptance.py \
  --project acceptance-fixture --output /physical/owned/parent/new-acceptance-kit
```

The package contains exactly three files:

- `fixture.bin`: 8 MiB of public synthetic data, 256 distinct 32-KiB chunks;
  chunk N consists of byte value N repeated 32,768 times. It is not code, a
  repository, a model or a user-data snapshot.
- `manifest.json`: the canonical SDK transfer manifest for the selected project.
  Its digest differs from the whole-file digest. Independently verify/share the
  expected digests before a real run; the generator's output grants no trust.
- `evidence.json`: fixture facts, empty required inputs, a proposed bounded plan,
  and per-step observations initialized to `not_run`. Actual timestamps, exit
  codes and reports stay empty until observed. Proposed timeout/budget/count
  values are not approvals or measured evidence.

The directory is created with mode 0700 and files with mode 0600. Existing paths
are never overwritten. I/O failure can leave a partial directory; inspect it and
choose a new output directory rather than assuming rollback. No disk durability
or hostile same-user filesystem isolation is promised. Generated binaries and
machine-specific evidence belong outside the repository and active app checkouts.

The clean interruption plan stops after three verified chunks, then expects 253
missing chunks on a new explicit resume **only if exactly those three checkpoints
remain valid**. Record actual counts rather than copying the plan. The existing
installed crash-recovery fixture has an additional recovered chunk and expects
252; these are different scenarios. Do not run the installed smoke suite as this
offline preparation step: that suite creates ephemeral identities and listeners.

Keep a pristine worksheet and fill a separate operator-owned evidence copy when
the required inputs and permissions are ready. The JSON is a human worksheet,
not a signed receipt, proof of distinct machines or automated acceptance verdict.

### Approved real-machine steps

Use the exact command templates in the [runbook](two-peer-operations.md), replacing
placeholders only from the completed input record. Do not execute placeholders.
Choose a finite manual attempt count before starting. Record UTC start/end,
direction, exit status and stdout/stderr separately for each step.

| Step | Required observation | Result |
| --- | --- | --- |
| Environment | Record both OS/Python/OpenSSL versions, TLS 1.3 availability, reviewed wheel hash and dependency selection. | Not run |
| Start A, check B → A | Authorized foreground readiness on A, then `check` on B succeeds with the intended project, pin, protocol and capabilities. Readiness alone is not reachability. | Not run |
| Rejection | B's command-only wrong-pin and wrong-project checks fail without changing stored trust or trying to repair it. Restore the approved arguments explicitly. | Not run |
| Transfer | B's selected bytes match the manifest and whole-file digest; success JSON reports integrity, not publisher authorization; output is private. | Not run |
| Interruption and restart | Interrupt before completion, retain checkpoints, end the client process and explicitly restart with `--resume`, the same manifest/staging and absent output. Rehashed cached chunks reduce fetched chunks. If the original transfer completed too quickly, record this step as not exercised. | Not run |
| Stop and reconnect | Stop A's task-owned foreground service; B's next explicit check fails within the configured attempt. Restart A explicitly and verify a new explicit connection. | Not run |
| Optional alternatives | Only if independently approved addresses for the same identity are available: use `--alternate HOST PORT SERVER_NAME`, demonstrate unavailable-primary fallback, and preserve one total timeout. Do not invent or probe unapproved ports. | Not run / not selected |
| Optional reverse direction | Separately approve B's listener and A's client role, supply suitable existing identities/allowlists and repeat. Forward success proves only the forward direction. | Not run / not selected |

For repeatable interruption without changing network settings, the public
[`download_transfer` API](download-api.md) supports cancelling from an awaited
progress callback after a selected verified-chunk count. Run it from the approved
installed environment, preserve checkpoints, end that client process and resume
in a new explicit invocation. Each invocation has its own finite deadline; a
server restart creates a new lifetime byte budget. Neither is an unlimited retry
authorization. Exact-budget process-restart behavior is already covered locally
by `scripts/peer_install_smoke.py`; it is not two-computer evidence.

## Repository claims and output interpretation

For an approved opaque bundle, use the merged claim-selection options:

```sh
aethermesh-peer bundle-inspect --project PROJECT_ID \
  --file received.bundle --sha256 EXPECTED_WHOLE_BUNDLE_SHA256 \
  --expected-ref refs/heads/SELECTED_BRANCH --expected-oid FULL_40_HEX_COMMIT_OID
```

Obtain the expectations independently of the received header. Matching claims
must still report `pack_validated: false` and `publisher_authorized: false`.
Mismatch must fail without changing the received file. Do not pass received
packs to Git, import, checkout, extract, execute or build them. Containment,
publisher/channel policy and trusted releases remain separate work.

Branch on CLI exit status before parsing output: success is one stdout JSON
record; operational errors are static stderr JSON; syntax errors and interruption
have different output rules. If an error includes `publication_attempted: true`,
complete output may already exist. Inspect the selected path/hash before retrying;
never treat an error as permission to replace or delete output. Absence of that
field does not rule out output created by another process.

## Close the record honestly

Save only operator-approved evidence: build identifiers, public pins, selected
digests, per-step status and sanitized reports. Redact private paths/topology as
appropriate. Leave failed, blocked and unexercised steps visible. Record whether
the two-machine distinction is operator-confirmed; SDK output does not attest
physical machine identity.

Stop task-owned foreground processes and leave checkpoints/output for the named
cleanup owner. Keep GitHub operational. AetherMesh and Eidolon retain separate
project IDs, publisher identities and release channels. Passing this worksheet
proves the recorded direct-peer scenario only, not a public network, discovery,
relay availability, safe Git ingestion, full NET-1/NET-2 completion or application
integration.
