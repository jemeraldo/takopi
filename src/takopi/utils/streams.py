from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import suppress
import sys
from typing import Any

import anyio
from anyio.abc import ByteReceiveStream
from anyio.streams.buffered import BufferedByteReceiveStream

from ..logging import log_pipeline


async def iter_bytes_lines(stream: ByteReceiveStream) -> AsyncIterator[bytes]:
    buffered = BufferedByteReceiveStream(stream)
    while True:
        try:
            line = await buffered.receive_until(b"\n", sys.maxsize)
        except anyio.IncompleteRead:
            return
        yield line


async def drain_stderr(
    stream: ByteReceiveStream,
    logger: Any,
    tag: str,
    on_line: Callable[[str], None] | None = None,
) -> None:
    try:
        async for line in iter_bytes_lines(stream):
            text = line.decode("utf-8", errors="replace")
            if on_line is not None:
                with suppress(Exception):
                    on_line(text)
            log_pipeline(
                logger,
                "subprocess.stderr",
                tag=tag,
                line=text,
            )
    except Exception as exc:  # noqa: BLE001
        log_pipeline(
            logger,
            "subprocess.stderr.error",
            tag=tag,
            error=str(exc),
        )
