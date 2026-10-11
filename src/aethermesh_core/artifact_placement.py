"""Pure, opt-in placement simulation with aggregate node payload budgets.

Plans do not reserve resources, create stores, contact peers, attest failure
independence, or promise durable retention. No learned controller exists here.
"""

from __future__ import annotations

from dataclasses import dataclass

from .artifact_codec import ArtifactManifest, _integer
from .network.config import validate_project_id
from .network.transfer import TransferManifest


@dataclass(frozen=True)
class NodeBudget:
    node_id: str
    operator: str
    failure_domain: str
    storage_bytes: int
    memory_bytes: int
    object_slots: int
    ingress_bytes: int
    egress_bytes: int
    max_inflight: int = 1

    def __post_init__(self) -> None:
        for label in (self.node_id, self.operator, self.failure_domain):
            validate_project_id(label)
        for value in (
            self.storage_bytes,
            self.memory_bytes,
            self.ingress_bytes,
            self.egress_bytes,
        ):
            _integer(value, 0, 64 * 1024 * 1024)
        _integer(self.object_slots, 0, 192)
        _integer(self.max_inflight, 0, 8)


@dataclass(frozen=True)
class PlacementLimits:
    min_operators: int = 2
    max_inflight: int = 2
    max_transfer_bytes: int = 16 * 1024 * 1024

    def __post_init__(self) -> None:
        _integer(self.min_operators, 1, 6)
        _integer(self.max_inflight, 1, 16)
        _integer(self.max_transfer_bytes, 0, 32 * 1024 * 1024)


@dataclass(frozen=True)
class NodeReservation:
    node_id: str
    storage_bytes: int
    objects: int
    memory_bytes: int
    transfer_bytes: int


@dataclass(frozen=True)
class PlacementPlan:
    manifest_digest: str
    status: str
    assignments: tuple[tuple[int, str], ...]
    batches: tuple[tuple[tuple[int, str], ...], ...]
    reservations: tuple[NodeReservation, ...]
    unplaced: tuple[int, ...]


def _transfer_cost(manifest: ArtifactManifest, index: int) -> int:
    size = manifest.fragment_size(index)
    # Hash values are fixed-width: this has exactly the real transfer manifest's
    # length. Budget units match TransferStore: raw response payload bytes only.
    transfer = TransferManifest(
        manifest.project,
        manifest.fragments[index][0],
        size,
        tuple("0" * 64 for _ in range((size + 32767) // 32768)),
    )
    return size + len(transfer.encode())


def _node_inventory(nodes: tuple[NodeBudget, ...]) -> dict[str, NodeBudget]:
    if (
        not isinstance(nodes, tuple)
        or len(nodes) > 32
        or any(not isinstance(node, NodeBudget) for node in nodes)
        or len({node.node_id for node in nodes}) != len(nodes)
    ):
        raise ValueError("invalid node inventory")
    return {node.node_id: node for node in nodes}


def plan_placement(
    manifest: ArtifactManifest,
    nodes: tuple[NodeBudget, ...],
    *,
    limits: PlacementLimits,
    now: int,
    deleted: bool = False,
) -> PlacementPlan:
    """Conservative deterministic plan, six configured domains per full stripe.

    Aggregate budgets must be free capacity AFTER existing work/reservations.
    The simulator does not coordinate concurrent planners or enforce live quotas.
    Each stripe is all-or-nothing; insufficient resources produce explicit
    degradation rather than silently reducing redundancy or domain separation.
    """
    _integer(now, 1, 4102444800)
    _node_inventory(nodes)
    if not isinstance(limits, PlacementLimits) or type(deleted) is not bool:
        raise ValueError("invalid placement policy")
    if deleted or now >= manifest.expires_at:
        return PlacementPlan(
            manifest.digest,
            "deleted" if deleted else "expired",
            (),
            (),
            (),
            tuple(range(len(manifest.fragments))),
        )
    # Per node: stored bytes, object count, transfer bytes, largest fragment.
    used = {node.node_id: (0, 0, 0, 0) for node in nodes}
    chosen: list[tuple[int, str]] = []
    unplaced: list[int] = []
    for start in range(0, len(manifest.fragments), 6):
        trial = dict(used)
        domains: set[str] = set()
        operators: set[str] = set()
        stripe: list[tuple[int, str]] = []
        for index in range(start, start + 6):
            candidates = sorted(
                nodes,
                key=lambda node: (
                    node.operator in operators
                    if len(operators) < limits.min_operators
                    else False,
                    -(node.storage_bytes - trial[node.node_id][0]),
                    node.node_id,
                ),
            )
            for node in candidates:
                if node.failure_domain in domains or node.max_inflight == 0:
                    continue
                stored, objects, transferred, largest = trial[node.node_id]
                size, cost = (
                    manifest.fragment_size(index),
                    _transfer_cost(manifest, index),
                )
                future = (
                    stored + size,
                    objects + 1,
                    transferred + cost,
                    max(largest, size),
                )
                memory = future[0] + 2 * future[3] * min(
                    node.max_inflight, limits.max_inflight
                )
                if (
                    future[0] > node.storage_bytes
                    or future[1] > node.object_slots
                    or memory > node.memory_bytes
                    or future[2] > min(node.ingress_bytes, node.egress_bytes)
                    or sum(value[2] for value in trial.values()) + cost
                    > limits.max_transfer_bytes
                ):
                    continue
                trial[node.node_id] = future
                stripe.append((index, node.node_id))
                domains.add(node.failure_domain)
                operators.add(node.operator)
                break
        if len(stripe) == 6 and len(operators) >= limits.min_operators:
            used = trial
            chosen.extend(stripe)
        else:
            unplaced.extend(range(start, start + 6))
    capacities = {node.node_id: node.max_inflight for node in nodes}
    pending = list(chosen)
    batches: list[tuple[tuple[int, str], ...]] = []
    while pending:
        batch: list[tuple[int, str]] = []
        counts: dict[str, int] = {}
        for assignment in tuple(pending):
            node_id = assignment[1]
            if (
                len(batch) < limits.max_inflight
                and counts.get(node_id, 0) < capacities[node_id]
            ):
                batch.append(assignment)
                pending.remove(assignment)
                counts[node_id] = counts.get(node_id, 0) + 1
        batches.append(tuple(batch))
    reservations = tuple(
        NodeReservation(
            node.node_id,
            used[node.node_id][0],
            used[node.node_id][1],
            used[node.node_id][0]
            + 2 * used[node.node_id][3] * min(node.max_inflight, limits.max_inflight),
            used[node.node_id][2],
        )
        for node in nodes
        if used[node.node_id][1]
    )
    return PlacementPlan(
        manifest.digest,
        "degraded" if unplaced else "complete",
        tuple(chosen),
        tuple(batches),
        reservations,
        tuple(unplaced),
    )


def _simulation_assignments(
    manifest: ArtifactManifest, plan: PlacementPlan, nodes: dict[str, NodeBudget]
) -> dict[int, str]:
    count = len(manifest.fragments)
    if not isinstance(plan.assignments, tuple) or len(plan.assignments) > count:
        raise ValueError("simulation assignment bound")
    placements: dict[int, str] = {}
    domains: set[tuple[int, str]] = set()
    for entry in plan.assignments:
        if not isinstance(entry, tuple) or len(entry) != 2:
            raise ValueError("invalid simulation assignment")
        index, node_id = entry
        _integer(index, 0, count - 1)
        if type(node_id) is not str or node_id not in nodes or index in placements:
            raise ValueError("unknown node or duplicate fragment assignment")
        domain = (index // 6, nodes[node_id].failure_domain)
        if domain in domains:
            raise ValueError("repeated failure domain in stripe")
        domains.add(domain)
        placements[index] = node_id
    if not isinstance(plan.unplaced, tuple) or len(plan.unplaced) > count:
        raise ValueError("simulation unplaced bound")
    for index in plan.unplaced:
        _integer(index, 0, count - 1)
    unplaced = set(plan.unplaced)
    if (
        len(unplaced) != len(plan.unplaced)
        or unplaced & placements.keys()
        or unplaced | placements.keys() != set(range(count))
    ):
        raise ValueError("inconsistent assignment coverage")
    return placements


def simulate_domains(
    manifest: ArtifactManifest,
    plan: PlacementPlan,
    nodes: tuple[NodeBudget, ...],
    *,
    offline: tuple[str, ...] = (),
    lost: tuple[str, ...] = (),
) -> tuple[str, ...]:
    """Point-in-time synthetic availability; not a durability probability.

    'insufficient_retained' includes never-placed fragments; it does not assert
    every copy of the original artifact has been permanently lost everywhere.
    """
    if not isinstance(plan, PlacementPlan) or plan.manifest_digest != manifest.digest:
        raise ValueError("simulation context mismatch")
    by_node = _node_inventory(nodes)
    domains = {node.failure_domain for node in nodes}
    for unavailable in (offline, lost):
        if (
            not isinstance(unavailable, tuple)
            or len(unavailable) > len(domains)
            or any(
                type(domain) is not str or domain not in domains
                for domain in unavailable
            )
            or len(set(unavailable)) != len(unavailable)
        ):
            raise ValueError("unknown or duplicate simulation domain")
    if set(offline) & set(lost):
        raise ValueError("overlapping offline and lost domains")
    placements = _simulation_assignments(manifest, plan, by_node)
    states = []
    for start in range(0, len(manifest.fragments), 6):
        available = retained = 0
        for index in range(start, start + 6):
            if index not in placements:
                continue
            domain = by_node[placements[index]].failure_domain
            if domain not in lost:
                retained += 1
                if domain not in offline:
                    available += 1
        states.append(
            "available"
            if available >= 4
            else "temporarily_unavailable"
            if retained >= 4
            else "insufficient_retained"
        )
    return tuple(states)
