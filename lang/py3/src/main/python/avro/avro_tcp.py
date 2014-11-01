#!/usr/bin/env python3
# -*- mode: python; coding: utf-8 -*-

"""Avro IPC client and server over multiplexed TCP connections."""

import collections
import io
import logging
import queue
import socket
import socketserver
import threading

from avro import io as avro_io
from avro import ipc


# --------------------------------------------------------------------------------------------------


class RequestContext(object):
    def __init__(self, request_id):
        self._request_id = request_id

    @property
    def request_id(self):
        return self._request_id


AvroResponse = collections.namedtuple(
    typename="AvroResponse",
    field_names=(
        "request_id",
        "message",
        "response_datum",
    )
)


AvroRequest = collections.namedtuple(
    typename="AvroRequest",
    field_names=(
        "message",
        "request_datum",
        "response_queue",
        "callback",
        "context",
    )
)


AvroRequestContext = collections.namedtuple(
    typename="AvroRequestContext",
    field_names=(
        "timeout",
    )
)


# --------------------------------------------------------------------------------------------------


def write_bytes(writer, bytes):
    """Writes an arbitrary byte array.

    The byte array is seralized, prefixed with a 32bits unsigned integer representing
    the byte array size.

    Args:
        writer: Writer to write bytes into.
        bytes: Byte array to write.
    """
    nbytes = len(bytes)
    ipc.write_uint32(writer, nbytes)
    written_count = writer.write(bytes)
    assert (written_count == nbytes), \
        ("Error while sending %d bytes, effectively sent %d bytes", nbytes, written_count)


def read_bytes(reader):
    """Reads an arbitrary byte array.

    Args:
        reader: Reader to read bytes from.
    Returns:
        The byte array received from the reader.
    """
    nbytes = ipc.read_uint32(reader)
    bytes = reader.read(nbytes)
    assert (len(bytes) == nbytes), \
        ("Error while reading %d bytes, effectively received %d bytes", nbytes, len(bytes))
    return bytes


# --------------------------------------------------------------------------------------------------


def write_request(encoder, message, request, meta=None):
    ipc.META_WRITER.write(meta, encoder)
    encoder.write_utf8(message.name)
    request_writer = avro_io.DatumWriter(message.request)
    request_writer.write(request, encoder)


# --------------------------------------------------------------------------------------------------


class AvroIpcTcpRequestHandler(socketserver.BaseRequestHandler):
    """TCP request handler for aan Avro IPC server.

    Several instances of this class will be created by the TCP socket server.
    Typically, one per active connection (although an existing instance could be reused).
    """

    @property
    def responder(self):
        raise Error("Abstract property")

    @property
    def avro_server(self):
        raise Error("Abstract property")

    @property
    def socket(self):
        """Returns: the socket for the TCP connection to handle."""
        return self._socket

    def setup(self):
        self._thread = threading.current_thread()
        logging.info("[%s:%s] setup() TCP connection: %r -> %r",
                     self._thread.name, self._thread.ident,
                     self.client_address, self.server.server_address)
        self._socket = self.request
        self._socket_io = socket.SocketIO(self._socket, mode="rw")

        self._encoder = avro_io.BinaryEncoder(self._socket_io)
        self._decoder = avro_io.BinaryDecoder(self._socket_io)

        # Map: request ID -> request context
        self._request = dict()

        self._response_thread = None
        self._response_queue = queue.Queue()

    def handle(self):
        logging.info("[%s:%s] handle() TCP connection: %r -> %r",
                     self._thread.name, self._thread.ident,
                     self.client_address, self.server.server_address)

        # Read handshake request:
        bytes = self._decoder.read_bytes()
        logging.info("Received handshake request (%d bytes)", len(bytes))
        rdecoder = avro_io.BinaryDecoder(io.BytesIO(bytes))
        handshake_req = ipc.HANDSHAKE_RESPONDER_READER.read(rdecoder)
        logging.info("Received handshake request %r", handshake_req)

        # TODO: Handle reader/writer protocols properly. For now, we assume client == server.

        # Send handshake response:
        handshake_rep = {  # HandshakeResponse
            "match": "BOTH",
            "serverProtocol": str(self.responder.local_protocol),
            "serverHash": self.responder.local_protocol.md5,
            "meta": {},
        }
        logging.info("Sending handshake response %r", handshake_rep)

        writer = io.BytesIO()
        wencoder = avro_io.BinaryEncoder(writer)
        ipc.HANDSHAKE_RESPONDER_WRITER.write(handshake_rep, wencoder)
        self._encoder.write_bytes(writer.getvalue())
        logging.debug("Handshake response sent")

        self._response_thread = threading.Thread(target=self._handle_response)
        self._response_thread.start()
        logging.info("Response thread started")

        # self.socket.settimeout(10.0)

        # Receives incoming requests from the socket and dispatch them to worker threads:
        while True:
            request_id = self._decoder.read_long()
            logging.debug("Processing request ID %r", request_id)

            meta = ipc.META_READER.read(self._decoder)
            message_name = self._decoder.read_utf8()
            message = self.responder.local_protocol.message_map[message_name]
            request_reader = avro_io.DatumReader(
                writer_schema=message.request,  # FIXME
                reader_schema=message.request,
            )
            request_datum = request_reader.read(self._decoder)
            logging.debug("Received request datum %r for request ID %r", request_datum, request_id)

            # TODO: dispatch the request to worker thread
            kwargs = dict(
                context=RequestContext(request_id=request_id),
                message=message,
                request=request_datum,
            )
            thread = threading.Thread(target=self._handle_request, kwargs=kwargs)
            #kwargs['thread'] = thread
            thread.start()

    def _handle_request(self, message, context, request):
        response = self.responder.invoke(
            message=message,
            request=request,
            context=context,
        )
        avro_response = AvroResponse(
            request_id=context.request_id,
            message=message,
            response_datum=response,
        )
        logging.info("Queueing response %r", avro_response)
        self._response_queue.put(avro_response)

    def _handle_response(self):
        while True:
            avro_response = self._response_queue.get()
            if avro_response is None:
                return

            self._encoder.write_long(avro_response.request_id)

            # Encode the response:
            writer = avro_io.DatumWriter(writer_schema=avro_response.message.response)
            writer.write(datum=avro_response.response_datum, encoder=self._encoder)
            logging.info("Sent response for request #%d", avro_response.request_id)

    def finish(self):
        logging.info("[%s:%s] finish() TCP connection: %r -> %r",
                     self._thread.name, self._thread.ident,
                     self.client_address, self.server.server_address)
        self._socket_io.close()
        self._socket_io = None
        self._socket.close()
        self._socket = None


# --------------------------------------------------------------------------------------------------


class AvroTcpServer(socketserver.ThreadingTCPServer):
    """Server for an Avro IPC server over multiplexed TCP connections."""

    def __init__(self, address, responder):
        self._address = address
        self._responder = responder
        avro_server = self

        class Handler(AvroIpcTcpRequestHandler):
            @property
            def responder(self):
                return responder

            @property
            def avro_server(self):
                return avro_server

        super().__init__(
            server_address=self._address,
            RequestHandlerClass=Handler,
        )


# --------------------------------------------------------------------------------------------------


class AvroTcpClient(object):
    """Client for an Avro IPC server over multiplexed TCP connections."""

    def __init__(self, protocol, host, port):
        self._protocol = protocol
        self._host = host
        self._port = port

        # Socket to the Avro TCP server:
        self._socket = None

        # Queue of requests to process:
        self._queue = queue.Queue()

        # Map: request ID -> AvroRequest in progress
        self._request = dict()

        self._requestor_thread = None
        self._receiver_thread = None

        def make_message_handler(message_name):
            message = protocol.message_map[message_name]

            def message_handler(request, callback=None, context=None):
                logging.info("Sending message %r with request %r", message_name, request)
                synchronous = (callback is None)

                # Queue to receive the response:
                response_queue = queue.Queue()

                if synchronous:
                    def sync_callback(context, reply, error):
                        response_queue.put((context, reply, error))

                    callback = sync_callback

                # Put request in the queue:
                request = AvroRequest(
                    message=message,
                    request_datum=request,
                    response_queue=response_queue,
                    callback=callback,
                    context=context,
                )
                self._queue.put(request)

                if synchronous:
                    timeout = None
                    if context is not None:
                        timeout = context.timeout
                    (reply, unused_context) = response_queue.get(timeout=timeout)
                    return reply
                else:
                    return None

            return message_handler

        for message in protocol.messages:
            assert (getattr(self, message.name, None) is None), \
                ("Conflict for message name %r" % message.name)
            setattr(self, message.name, make_message_handler(message.name))

    def open(self):
        self._open_connection()

    def close(self):
        # TODO: there is a lot more cleanup to do...

        self._socket.close()
        self._queue.put(None)
        self._requestor_thread.join()
        self._receiver_thread.join()

        self._socket = None
        self._requestor_thread = None
        self._receiver_thread = None

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def protocol(self):
        """Returns: the Avro protocol for this client."""
        return self._protocol

    def _open_connection(self):
        self._socket = socket.socket()
        self._socket.connect((self._host, self._port))
        self._socket_io = socket.SocketIO(self._socket, mode="rw")

        self._encoder = avro_io.BinaryEncoder(self._socket_io)
        self._decoder = avro_io.BinaryDecoder(self._socket_io)

        # Send handshake request:
        handshake_req = {  # HandshakeRequest
            "clientHash": self._protocol.md5,
            "clientProtocol": str(self._protocol),
            "serverHash": self._protocol.md5,
            "meta": None,
        }
        logging.debug("Sending handshake request: %s", handshake_req)

        writer = io.BytesIO()
        encoder = avro_io.BinaryEncoder(writer)
        ipc.HANDSHAKE_REQUESTOR_WRITER.write(handshake_req, encoder)
        bytes = writer.getvalue()
        self._encoder.write_bytes(bytes)
        logging.debug("Handshake request sent (%d bytes)", len(bytes))

        # Read handshake response:
        bytes = self._decoder.read_bytes()
        decoder = avro_io.BinaryDecoder(io.BytesIO(bytes))
        handshake_req = ipc.HANDSHAKE_REQUESTOR_READER.read(decoder)
        logging.info("Received handshake response %r", handshake_req)

        self._requestor_thread = threading.Thread(target=self._requestor_loop)
        self._requestor_thread.start()
        self._receiver_thread = threading.Thread(target=self._receiver_loop)
        self._receiver_thread.start()

    def _requestor_loop(self):
        """Requestor loop running in a dedicated thread.

        Requests are sent with a unique request ID, in the order they appear in the queue.
        Replies are received out of order.
        """
        self._next_request_id = 1
        while True:
            request_id = self._next_request_id
            self._next_request_id += 1

            avro_request = self._queue.get()
            if avro_request is None:
                logging.debug("Exiting Avro client requestor loop")
                return

            self._request[request_id] = avro_request

            self._encoder.write_long(request_id)
            write_request(
                encoder=self._encoder,
                message=avro_request.message,
                request=avro_request.request_datum,
                meta={},
            )

    def _receiver_loop(self):
        while True:
            request_id = self._decoder.read_long()
            logging.info("Receiving response for request #%d", request_id)
            avro_request = self._request[request_id]
            logging.info("Receiving response with schema: %s", avro_request.message.response)
            reader = avro_io.DatumReader(
                writer_schema=avro_request.message.response,  # FIXME
                reader_schema=avro_request.message.response,
            )
            response = reader.read(self._decoder)
            avro_request.callback(response)
