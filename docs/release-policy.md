# SDK package and release boundary

The SDK repository no longer builds or publishes desktop installers. The old
weekly desktop publication workflow and desktop-only composite action have been
removed. Existing remote releases/tags are historical and are not modified by
this cleanup. No replacement publication schedule is enabled here.

SDK CI continues to validate Python code, build/install the package and inspect
artifact provenance. `scripts/release_metadata.py` and `scripts/release_policy.py`
retain testable source metadata and legacy numbered-release/check-policy helpers.
Their presence does not trigger a release or establish a P2P update channel.
The recorded required checks are repository policy, not a live read of GitHub
branch protection settings.

A future SDK release process should pin the reviewed commit, publish wheel and
source artifacts with verification evidence, and maintain SDK/protocol version
compatibility. Project release channels and authority must remain separate from
Eidolon, including when both use the same transport.

Peer-hosted artifacts, signed project manifests, freshness/rollback checks,
independent build verification and evidence-based AI promotion recommendations
remain [design proposals](sdk-scope.md#design-proposals-not-implemented-guarantees).
Native application install, relaunch and rollback belong to the application.
Do not treat the retained legacy pip updater as implementation of these policies.
