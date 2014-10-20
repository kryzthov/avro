#!/usr/bin/env python3
# -*- mode: python; coding: utf-8 -*-

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""There are currently no IPC tests within python."""

import logging
import threading
import time
import unittest

from avro import avro_tcp
from avro import ipc
from avro import protocol
from avro import schema


def now_ms():
    return int(time.time() * 1000)


ECHO_PROTOCOL_JSON = """
{
    "protocol" : "Echo",
    "namespace" : "org.apache.avro.ipc.echo",
    "types" : [ {
        "type" : "record",
        "name" : "Ping",
        "fields" : [ {
            "name" : "timestamp",
            "type" : "long",
            "default" : -1
        }, {
            "name" : "text",
            "type" : "string",
            "default" : ""
        } ]
    }, {
        "type" : "record",
        "name" : "Pong",
        "fields" : [ {
            "name" : "timestamp",
            "type" : "long",
            "default" : -1
        }, {
            "name" : "ping",
            "type" : "Ping"
        } ]
    } ],
    "messages" : {
        "ping" : {
            "request" : [ {
                "name" : "ping",
                "type" : "Ping"
            } ],
            "response" : "Pong"
        }
    }
}
"""


ECHO_PROTOCOL = protocol.parse(ECHO_PROTOCOL_JSON)


class EchoResponder(ipc.AvroResponder):
    def __init__(self):
        super().__init__(protocol=ECHO_PROTOCOL)

    def ping(self, context, request):
        """Implements the ping protocol message.

        Args:
            context: RPC context.
            request: Ping request.
        Returns:
            Pong Avro record.
        """
        ping = request["ping"]
        time.sleep(1.0)
        return {  # Pong
            'timestamp': now_ms(),
            'ping': ping,
        }


# --------------------------------------------------------------------------------------------------


class TestAvroTcp(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Reference to an Echo RPC over HTTP server:
        self._server = None

    def start_echo_server(self):
        self._server = avro_tcp.AvroTcpServer(
            address=("localhost", 0),
            responder=EchoResponder(),
        )

        def server_thread():
            self._server.serve_forever()

        self._server_thread = threading.Thread(target=server_thread)
        self._server_thread.start()

        logging.info('Echo RPC Server listening on %s:%s', *self._server.server_address)
        logging.info('RPC socket: %s', self._server.socket)

    def stop_echo_server(self):
        assert (self._server is not None)
        self._server.shutdown()
        self._server_thread.join()
        self._server.server_close()
        self._server = None

    def test_echo_service(self):
        """Tests client-side of the Echo service."""
        self.start_echo_server()
        try:
            (server_host, server_port) = self._server.server_address
            client = avro_tcp.AvroTcpClient(
                protocol=ECHO_PROTOCOL,
                host=server_host,
                port=server_port,
            )
            client.open()

            try:
                def ping_callback(response):
                    logging.info("Received ping response: %r", response)

                response = client.ping(
                    request={'ping': {'timestamp': 31415, 'text': 'hello ping'}},
                    callback=ping_callback,
                )
                logging.info('Received echo response: %s', response)

                time.sleep(0.5)

                response = client.ping(
                    request={'ping': {'timestamp': 218456, 'text': 'hello bongbong'}},
                    callback=ping_callback,
                )
                logging.info('Received echo response: %s', response)

                time.sleep(3.0)
            # response = requestor.request(
            #     message_name='ping',
            #     request_datum={'ping': {'timestamp': 123456, 'text': 'hello again'}},
            # )
            # logging.info('Received echo response: %s', response)



            finally:
                client.close()

        finally:
            self.stop_echo_server()


if __name__ == '__main__':
    raise Exception('Use run_tests.py')