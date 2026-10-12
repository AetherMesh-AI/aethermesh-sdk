# Deferred ownership-restoration gate

**Status: blocked by an unresolved authority and rollback design.** This is a
separate recovery gate, not an executable procedure or an implemented restoration
API. Intact-binding inventory and quarantine may proceed without solving it.
Missing/corrupt ownership evidence must continue to preserve bytes and full charges.

The version-1 ledger claims a directory device/inode. The intact directory marker
pins database device/inode, store ID and frozen policy. A copied database carries
the same directory claim, but its inode differs and the original marker rejects
it. If the marker is missing, current appearances cannot distinguish an original
owner from that copy. Restoring a claim from filenames or successful content hashes
has the same authority problem. Modification time, inactivity and expiry establish
neither abandonment nor ownership. Ledger checksums detect accidental corruption;
they do not authenticate an operator or prove that a snapshot is the newest one.

Before any restoration/migration API can be designed, decide all of the following:

1. **Original evidence and authority.** What evidence was preserved before damage,
   where is its integrity protected, and which explicitly selected local operator
   is authorized to map it to the damaged store? It must bind the original store,
   frozen resource policy, ledger/directory identities and admission state. A copy
   of current damaged files or a newly generated marker is not original evidence.
   Do not silently convert an unsigned observation into a restoration credential.
2. **Rollback and duplication.** How is freshness established without trusting
   wall-clock age or whichever database has the largest apparent revision? How
   are outstanding charges and writer authority handled if several copies exist?
   Restoring an old snapshot must not drop later reservations or create multiple
   active writers against one resource budget. Inode numbers can be reused;
   retained inode values alone are not a durable anti-rollback design.
3. **Writer quiescence and failure protocol.** Which cooperating writer scope can
   be positively fenced, and how does explicit migration preserve evidence and
   charges through interruption before/after each ownership transition? A timeout
   is not proof that a writer stopped. Do not stop unrelated applications or
   rewrite bindings while another owner may still be active.
4. **Validation boundary.** Define copied-ledger, replaced-directory, missing and
   truncated-marker, partial-initialization, concurrent-writer, stale-backup and
   crash/rollback tests before implementation. Establish supported local filesystem
   semantics separately from power-loss or multi-machine claims.

No new credentials, company key, online authority, distributed lease or trust
expansion is selected by this document. Choosing such a mechanism would require
its own explicit design and validation. Until the questions above have concrete
answers and review, restoration remains unsupported: no marker regeneration,
claim reconstruction, automatic migration, deletion or charge release.

The current [owned inventory audit](artifact-owned-inventory.md) and
[explicit quarantine](artifact-quarantine.md) require the intact original binding.
They do not grant a path around this gate.
