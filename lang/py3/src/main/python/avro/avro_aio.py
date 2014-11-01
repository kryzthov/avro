#!/usr/bin/env python3
# -*- mode: python; coding: utf-8 -*-

"""Avro IPC client and server over multiplexed TCP connections."""

import asyncio
import collections
import io
import logging
import queue
import socket
import threading

from avro import io as avro_io
from avro import ipc

# --------------------------------------------------------------------------------------------------


class AvroTcpProtocol(asyncio.StreamReaderProtocol):
    """AsyncIO handler to process a new TCP connection."""

    def __init__(self, loop, protocol):
        """Initializes a new TCP connection handler.

        Args:
            loop: AsyncIO loop to use.
            protocol: Avro Protocol to handle.
        """
        self._loop = loop
        self._protocol = protocol
        self._transport = None

        self._stream_reader = asyncio.StreamReader(loop=loop)
        super().__init__(
            stream_reader=self._stream_reader,
            loop=loop,
        )

    @property
    def loop(self):
        """Returns: the main loop to schedule events against."""
        return self._loop

    @property
    def stream_reader(self):
        """Returns: the stream reader for the TCP connection this protocol wraps."""
        return self._stream_reader

    # --------------------------------------------------------------------------

    def connection_made(self, transport):
        self._transport = transport

        socket = transport.get_extra_info("socket")
        logging.info("New connection established: %r (%r)", transport, socket)

        transport.write(b'Hello')
        super().connection_made(transport)

        asyncio.async(self.process_input_stream(), loop=self.loop)

    def connection_lost(self, exc):
        logging.info("Connection lost: %r", exc)
        super().connection_lost(exc)

    # --------------------------------------------------------------------------

    @asyncio.coroutine
    def process_input_stream(self):
        while True:
            read = yield from self.stream_reader.read(n=10)
            logging.info("Read = %r", read)

    def data_received(self, data):
        logging.info("Received bytes: %r", data)
        super().data_received(data)

    def eof_received(self):
        logging.info("Connection closed")
        super().eof_received()


# --------------------------------------------------------------------------------------------------


def make_avro_server(loop, protocol, interface, port):
    def protocol_factory():
        return AvroTcpProtocol(loop)

    server_coroutine = loop.create_server(
        protocol_factory=protocol,
        host=interface,
        port=port,
        family=socket.AF_INET,
        backlog=100,
        reuse_address=True,
    )
    server = loop.run_until_complete(server_coroutine)

    logging.info("Serving on %s", server.sockets[0].getsockname())

    return server
