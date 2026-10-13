"""Adversarial local connector verification; no live-network acceptance claim."""

import asyncio
import unittest
from dataclasses import FrozenInstanceError, replace
from unittest.mock import AsyncMock, MagicMock, patch

from aethermesh_core.network import (
    AuthenticationError,
    ConnectionClosed,
    EndpointTimeout,
    EndpointUnavailable,
    ProtocolError,
    RequestTimeout,
    candidates,
)
from tests.test_network_candidates import ENDPOINTS, IDENTITY


class AdversarialCandidateTests(unittest.IsolatedAsyncioTestCase):
    async def call(self):
        return await candidates.connect_peer_candidates(
            IDENTITY,
            ENDPOINTS + (replace(ENDPOINTS[1], port=12347),),
            project_id="test",
            timeout=1,
        )

    async def test_order_preserved_and_terminal_middle_failure_blocks_last_candidate(
        self,
    ):
        for failure in (
            AuthenticationError("identity"),
            ProtocolError("protocol"),
            ConnectionClosed("negotiation EOF"),
            RequestTimeout("negotiation deadline"),
        ):
            with self.subTest(failure=type(failure).__name__):
                clients = [
                    MagicMock(
                        connect=AsyncMock(side_effect=EndpointUnavailable("first")),
                        close=AsyncMock(),
                    ),
                    MagicMock(
                        connect=AsyncMock(side_effect=failure), close=AsyncMock()
                    ),
                    MagicMock(connect=AsyncMock(), close=AsyncMock()),
                ]
                with (
                    patch.object(
                        candidates, "PeerClient", side_effect=clients
                    ) as create,
                    self.assertRaises(type(failure)),
                ):
                    await self.call()
                self.assertEqual(create.call_count, 2)
                self.assertEqual(clients[0].connect.await_args.args, (ENDPOINTS[0],))
                self.assertEqual(clients[1].connect.await_args.args, (ENDPOINTS[1],))
                for client in clients[:2]:
                    client.close.assert_awaited_once()
                    client.request.assert_not_called()
                clients[2].connect.assert_not_called()

    async def test_cleanup_failure_is_terminal_even_if_its_type_is_retryable(self):
        for cleanup in (
            RuntimeError("cleanup"),
            TimeoutError("cleanup timeout before shared deadline"),
            EndpointUnavailable("cleanup unavailable"),
            EndpointTimeout("cleanup timeout"),
        ):
            with self.subTest(cleanup=type(cleanup).__name__):
                client = MagicMock(
                    connect=AsyncMock(side_effect=EndpointUnavailable("connect")),
                    close=AsyncMock(side_effect=cleanup),
                )
                with (
                    patch.object(
                        candidates, "PeerClient", return_value=client
                    ) as create,
                    self.assertRaises(type(cleanup)),
                ):
                    await self.call()
                create.assert_called_once()
                client.close.assert_awaited_once()

    async def test_cleanup_consumes_original_deadline(self):
        now = asyncio.get_running_loop().time()
        clock = [now]

        async def cleanup():
            clock[0] += 2

        client = MagicMock(
            connect=AsyncMock(side_effect=EndpointTimeout("first")),
            close=AsyncMock(side_effect=cleanup),
        )
        loop = MagicMock(time=lambda: clock[0])
        with (
            patch.object(candidates.asyncio, "get_running_loop", return_value=loop),
            patch.object(candidates, "PeerClient", return_value=client) as create,
            self.assertRaises(RequestTimeout),
        ):
            await self.call()
        create.assert_called_once()

    async def test_cancellation_during_retry_cleanup_stops_ordering(self):
        entered = asyncio.Event()

        async def cleanup():
            entered.set()
            await asyncio.Event().wait()

        client = MagicMock(
            connect=AsyncMock(side_effect=EndpointUnavailable("first")),
            close=AsyncMock(side_effect=cleanup),
        )
        with patch.object(candidates, "PeerClient", return_value=client) as create:
            task = asyncio.create_task(self.call())
            await asyncio.wait_for(entered.wait(), 1)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertTrue(task.cancelled())
        create.assert_called_once()

    async def test_cleanup_exception_does_not_replace_primary_cancellation_or_authentication(
        self,
    ):
        for failure in (asyncio.CancelledError(), AuthenticationError("pin mismatch")):
            with self.subTest(failure=type(failure).__name__):
                cleanup = RuntimeError("cleanup failed")
                client = MagicMock(
                    connect=AsyncMock(side_effect=failure),
                    close=AsyncMock(side_effect=cleanup),
                )
                with (
                    patch.object(
                        candidates, "PeerClient", return_value=client
                    ) as create,
                    self.assertRaises(type(failure)) as caught,
                ):
                    await self.call()
                self.assertIs(caught.exception, failure)
                self.assertIs(caught.exception.__cause__, cleanup)
                create.assert_called_once()

    async def test_second_cancellation_during_terminal_cleanup_never_falls_back(self):
        connecting, cleaning = asyncio.Event(), asyncio.Event()

        async def connect(endpoint):
            connecting.set()
            await asyncio.Event().wait()

        async def cleanup():
            cleaning.set()
            await asyncio.Event().wait()

        client = MagicMock(
            connect=AsyncMock(side_effect=connect), close=AsyncMock(side_effect=cleanup)
        )
        with patch.object(candidates, "PeerClient", return_value=client) as create:
            task = asyncio.create_task(self.call())
            await asyncio.wait_for(connecting.wait(), 1)
            task.cancel()
            await asyncio.wait_for(cleaning.wait(), 1)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        create.assert_called_once()
        self.assertTrue(task.cancelled())

    async def test_normal_endpoint_mutation_is_rejected_and_replacement_is_revalidated(
        self,
    ):
        endpoint = replace(ENDPOINTS[0])
        with self.assertRaises(FrozenInstanceError):
            endpoint.fingerprint = "b" * 64
        with self.assertRaises(TypeError):
            ENDPOINTS[0] = endpoint
        with (
            patch.object(candidates, "PeerClient") as create,
            self.assertRaises(ValueError),
        ):
            await candidates.connect_peer_candidates(
                IDENTITY,
                (endpoint, replace(ENDPOINTS[1], fingerprint="b" * 64)),
                project_id="test",
                timeout=1,
            )
        create.assert_not_called()

    async def test_actual_task_cancellation_and_total_timeout_survive_cleanup_failure(
        self,
    ):
        for external in (True, False):
            with self.subTest(external=external):
                entered = asyncio.Event()

                async def stalled(endpoint, entered=entered):
                    entered.set()
                    await asyncio.Event().wait()

                client = MagicMock(
                    connect=AsyncMock(side_effect=stalled),
                    close=AsyncMock(side_effect=RuntimeError("cleanup")),
                )
                with patch.object(
                    candidates, "PeerClient", return_value=client
                ) as create:
                    task = asyncio.create_task(
                        candidates.connect_peer_candidates(
                            IDENTITY,
                            ENDPOINTS,
                            project_id="test",
                            timeout=1 if external else 0.02,
                        )
                    )
                    await asyncio.wait_for(entered.wait(), 1)
                    if external:
                        task.cancel()
                    with self.assertRaises(
                        asyncio.CancelledError if external else RequestTimeout
                    ):
                        await task
                self.assertEqual(task.cancelled(), external)
                create.assert_called_once()

    async def test_terminal_nonbudget_timeout_is_not_relabeled(self):
        failure = TimeoutError("unexpected local timeout")
        client = MagicMock(connect=AsyncMock(side_effect=failure), close=AsyncMock())
        with (
            patch.object(candidates, "PeerClient", return_value=client) as create,
            self.assertRaises(TimeoutError) as caught,
        ):
            await self.call()
        self.assertIs(caught.exception, failure)
        create.assert_called_once()
        client.close.assert_awaited_once()
