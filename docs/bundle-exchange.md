# Trusted local bundle export and opaque header inspection

These optional installed CLI utilities help prepare repository bytes for the
existing authenticated peer transfer. They do not import objects, clone, fetch,
checkout, move trusted refs, execute hooks or build downloaded code. Keep GitHub
operational. An exported repository is not a release or publisher authorization.

## Export one explicitly trusted local branch

```sh
aethermesh-peer bundle-export --project aethermesh-sdk \
  --source /physical/path/to/trusted/repository \
  --ref refs/heads/main --expected-oid FULL_LOWERCASE_40_HEX_COMMIT_OID \
  --output new.bundle --timeout 15
```

The source must be an explicitly trusted local complete SHA-1 object store, not
peer-supplied packs or an untrusted downloaded repository. Ordinary repositories
with a directory `.git` and bare repositories are supported. Linked worktrees,
symlinked Git directories, shallow repositories, partial/promisor repositories,
commondir indirection, grafts, alternates and config includes are rejected.
Unsupported repository extensions and nonzero repository-format versions are
rejected. Source config is parsed as bounded data with `--no-includes`; it is never
loaded as the exporter Git process's runtime configuration.

Only conservative full `refs/heads/...` names are accepted. Short names, `HEAD`,
symbolic refs, revision expressions, ambiguous packed refs, noncanonical OIDs and
non-commit objects fail closed. The selected ref must equal the supplied commit
OID. The exporter then packs that fixed OID, not a moving ref. Concurrent source
ref changes cannot silently substitute a newer tip. No source refs are written. Output inside the source Git metadata directory is
rejected, including a new path under its refs or objects. Bare-repository exports
must therefore target a location outside that repository directory.

The optional system Git executable is resolved from the OS default executable
path, not a peer or the caller's custom PATH. Git runs in a fresh private scratch
bare repository with our configuration and empty hooks directory. Its environment
is an allowlist: inherited `GIT_*`, executable overrides and configuration counts
are not forwarded. Global/system config, prompts, replacement objects, lazy
fetching, protocols, fsmonitor and automatic maintenance are disabled. User home
configuration and source hooks/filter configuration are not loaded. No checkout,
index operations, attribute filters, source worktree files or application code
are executed. The trusted installed Git/Python runtimes remain part of the trust
boundary.

A fixed `pack-objects --stdout --revs --threads=1 --window-memory=32m` invocation
receives only the validated commit OID on stdin, with no thin-pack, filter or
exclusion options. The utility prepends the small one-ref bundle-v2 header. This
produces a self-contained bundle of the selected commit's reachable history,
subject to the 8-MiB output limit. It does not default to all branches or tags.

## Transfer and inspect

Share `new.bundle` using `serve --share-transfer` and obtain it with
`fetch-transfer`, as described in [resumable content](resumable-content.md).
Choose the project, peer pin and expected transfer manifest through a separately
trusted channel. After receiving the opaque file:

```sh
aethermesh-peer bundle-inspect --project aethermesh-sdk \
  --file received.bundle --sha256 EXPECTED_BUNDLE_SHA256
```

To require the header to advertise the separately selected branch and commit,
also provide `--expected-ref refs/heads/SELECTED_BRANCH --expected-oid FULL_OID`.
The SDK `inspect_bundle` accepts the equivalent optional `expected_ref` and
`expected_oid` keyword arguments. Both must be supplied together, pass the same
conservative branch/OID validation as export, and exactly match the header.
A mismatch fails with no success output; the file remains untouched. Omitting
both preserves inspection-only behavior and the existing output schema.

Matching these fields checks **claims only**. Even an invalid opaque pack can
advertise the selected values. Success does not prove the advertised commit is
present, validate object contents or history, authorize a publisher, establish
freshness/rollback policy, or permit checkout/execution. Select the expected
digest, ref and OID through an independent trusted process, not from the received
header itself. `pack_validated` and `publisher_authorized` remain false.

Inspection performs no Git invocation, decompression, object parsing or ingestion.
It reads at most 8 MiB, verifies the explicitly selected SHA-256, and parses a
header limited to 16 KiB. Only a bundle-v2 header advertising one conservative
SHA-1 branch ref and no prerequisites is accepted. Unknown versions/capabilities,
multiple refs, prerequisite lines, invalid encodings and malformed fields fail.
The opaque payload must begin with `PACK`; that marker does not validate a pack.
An invalid pack with a syntactically accepted header can still be reported.

Both commands return JSON containing `project`, `sha256`, `size`, `ref`,
`claimed_oid`, `bundle_version: 2`, `object_format: "sha1"`,
`pack_validated: false` and `publisher_authorized: false`. Header OIDs are claims;
inspection does not prove the pack contains the commit or a complete graph.
SHA-256 content integrity is separate from Git's SHA-1 object identities, TLS peer
identity and project publisher/channel authority. AetherMesh and Eidolon retain
separate publisher identities and release channels.

## Bounds and failure behavior

- Bundle output/input: 8 MiB. Header: 16 KiB. Source config: 64 KiB. Loose ref:
  64 bytes. Packed refs: 1 MiB. Object-store traversal: 10,000 entries and at most
  two path components below `objects`; symlinks/special files are rejected.
- Each Git process receives CPU, regular-file-size and descriptor limits before
  `exec`, plus the remaining shared subprocess wall deadline (default 15 seconds,
  maximum 30). Stdout/stderr go to temporary regular files, not unbounded memory
  pipes. Each is limited to 64 KiB for metadata commands or the remaining bundle
  size for packing; worst-case pack capture is below 16 MiB across both streams.
- The fixed Python limit wrapper execs the trusted Git builtin in the same process
  and process group. No shell or detached session is used. A timeout/failure
  produces a static CLI error and no published output.
- These are bounds for **trusted local export**, not a hostile-code sandbox. The
  Git delta-window setting is not a hard memory ceiling. No aggregate hostile
  process-tree memory/disk containment, network sandbox or untrusted ingestion is
  provided. No Docker, cgroup, firewall or security settings are configured.
- Local enumeration, file reads, hashing and fsync are synchronous and not subject
  to a hard disk-I/O deadline. Source directories and parents must be trusted and
  stable; this is not isolation against hostile same-user filesystem races.
- POSIX safe-file/resource-limit support and a trusted system Git are required for
  export. Header parsing itself has no Git dependency. CLI paths reject `..` and
  symlink components; use physical paths (e.g. `/private/tmp` on macOS).
- Output uses the existing private, atomic, no-overwrite publication and fsync
  path. Existing files are never replaced. The hard link is the irreversible
  commit point; a later error may leave complete output, and a crash can leave a
  temporary file. No rollback or universal filesystem durability is promised.

## Acceptance and future boundary

Tests use only locally generated disposable repositories. They cover malformed
headers and opaque invalid payloads, source config/hook/filter traps, inherited
Git environment, unsupported sources, ambiguous refs, commit pinning across a ref
race, subprocess deadline/output failures, unchanged source refs and private
no-overwrite output. Installed-wheel acceptance exports an owned fixture, moves
its opaque bytes between loopback peer processes, and inspects the received
header without object ingestion. The 8-MiB resume fixture uses distinct per-index
chunks to detect ordering errors.

This is not general decentralized Git or Internet/two-machine validation. A
separate reviewed slice must establish enforced memory, process, disk, filesystem
and network containment before any peer-supplied pack is passed to Git. Even
then, object validation would not authorize checkout, branch promotion or release
installation. Those remain explicit local policy decisions.
