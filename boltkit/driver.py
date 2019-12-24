#!/usr/bin/env python
# coding: utf-8

# Copyright (c) 2002-2016 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
******************************
How To Build a Neo4j Connector
******************************

Welcome to our simple guide to how to build a Bolt connector for Neo4j. This
file is intended to be read from top to bottom, giving an incremental
description of the pieces required to construct a simple connector from scratch
in any language.

While this connector implementation forms part of our suite of testing tools,
it also gives a great opportunity to illustrate the structure of a connector.
This is due to the inherent readability of Python source code as well as
Python's comprehensive standard library and the fact that minimal boilerplate
is required.

Note that while this connector is complete, it is neither supported, nor
intended for use in a production environment. You can of course decide to
ignore this and do so anyway but if you do, you're on your own!

So, let's get started.

Neo4j provides a binary protocol, called Bolt, which is what we are actually
targeting here. A Bolt connector will typically implement two layers of the
OSI network model:

Presentation Layer:
   For this, we use a custom serialisation format called PackStream. While the
   design of this format is inspired heavily by MessagePack, it is not
   compatible with it. PackStream provides a type system that is fully
   compatible with the Cypher type system used by Neo4j and also takes
   extension data types in a different direction to MessagePack. The PackStream
   implementation can be found in the separate `packstream` module.

Application Layer:
   At its heart, the Bolt protocol provides a stateful request-response
   mechanism. Each request consists of a textual statement plus a map or dictionary of
   parameters; each response is comprised of a stream of content plus some summary metadata.
   Message pipelining comes for free: a Bolt server will queue requests and respond to them in
   the same order in which they were received. A Bolt client therefore has a degree of
   flexibility in how and when it sends requests and how and when it gathers the responses.

"""

# You'll need to make sure you have the following items handy...
from logging import getLogger
from socket import socket, AF_INET, AF_INET6
from struct import pack as raw_pack, unpack_from as raw_unpack
from time import sleep
from timeit import default_timer as timer

# ...and we'll borrow some things from other modules
from boltkit.addressing import AddressList
from boltkit.packstream import UINT_16, UINT_32, Structure, pack, unpack


# CHAPTER 2: CONNECTIONS
# ======================
# It's here that we get properly into the protocol. A Bolt connection operates under a
# client-server model whereby a client sends requests to a server and the server responds
# accordingly. There is no mechanism for a server to send a message at any other time, nor should
# client software receive data when no message is expected.

# The Handshake
# -------------
# On successfully establishing a TCP connection to a listening server, the client should send a
# fixed sequence of four bytes in order to declare to the server that it intends to speak Bolt.
#
# Fun fact: the four "magic" bytes are supposed to spell "GO GO BOLT!" when written in hex and
# read through squinted eyes. Other hilarious phrases were considered and rejected.
#
BOLT = b"\x60\x60\xB0\x17"

# After the protocol announcement comes version negotiation. The client proposes to the server up
# to four protocol versions that it is able to support in order of preference. Each protocol
# version is a 32-bit unsigned integer and zeroes are used to fill the gaps if the client knows
# fewer than four versions.
#
# In response, the server replies with a single version number on which it agrees. If for some
# reason it can't agree, it returns a zero and disconnects immediately.
#
# Now that you've read and absorbed all that, push it to the back of your brain. As there currently
# exists only one version of Bolt, the negotiation turns out to be far more simple than that. The
# server and client send the following to each other:
#
#   C: 00:00:00:01 00:00:00:00 00:00:00:00 00:00:00:00
#   S: 00:00:00:01
#
# And that's it. If the server responds with anything else, hang up and get in touch. We may have a
# problem.
#

# Messaging
# ---------
# Once we've negotiated the (one and only) protocol version, we fall into the regular protocol
# exchange. This consists of request and response messages, each of which is serialised as a
# PackStream structure with a unique tag byte.
#
# The client sends messages from the selection below:
MAX_BOLT_VERSION = 4
CLIENT = [None] * (MAX_BOLT_VERSION + 1)
CLIENT[1] = {
    "INIT": 0x01,           # INIT <user_agent> <auth_token>
                            # -> SUCCESS - connection initialised
                            # -> FAILURE - init failed, disconnect (reconnect to retry)
                            #
                            # Initialisation is carried out once per connection, immediately
                            # after version negotiation. Before this, no other messages may
                            # validly be exchanged. INIT bundles with it two pieces of data:
                            # a user agent string and a map of auth information. More detail on
                            # on this can be found in the ConnectionSettings class below.

    "ACK_FAILURE": 0x0E,    # ACK_FAILURE
                            # -> SUCCESS - failure acknowledged
                            # -> FAILURE - protocol error, disconnect
                            #
                            # When a FAILURE occurs, no further actions may be carried out
                            # until that failure has been acknowledged by the client. This
                            # is a safety mechanism to prevent actions from being carried out
                            # by the server when several requests have been optimistically sent
                            # at the same time.

    "RESET": 0x0F,          # RESET
                            # -> SUCCESS - connection reset
                            # -> FAILURE - protocol error, disconnect
                            #
                            # A RESET is used to clear the connection state back to how it was
                            # immediately following initialisation. Specifically, any
                            # outstanding failure will be acknowledged, any result stream will
                            # be discarded and any transaction will be rolled back. This is
                            # used primarily by the connection pool.

    "RUN": 0x10,            # RUN <statement> <parameters>
                            # -> SUCCESS - statement accepted
                            # -> FAILURE - statement not accepted
                            # -> IGNORED - request ignored (due to prior failure)
                            #
                            # RUN is used to execute a Cypher statement and is paired with
                            # either PULL_ALL or DISCARD_ALL to retrieve or throw away the
                            # results respectively.
                            # TODO

    "DISCARD_ALL": 0x2F,    # DISCARD_ALL
                            # -> SUCCESS - result discarded
                            # -> FAILURE - no result to discard
                            # -> IGNORED - request ignored (due to prior failure)
                            #
                            # TODO

    "PULL_ALL": 0x3F,       # PULL_ALL
                            # .. [RECORD*] - zero or more RECORDS may be returned first
                            # -> SUCCESS - result complete
                            # -> FAILURE - no result to pull
                            # -> IGNORED - request ignored (due to prior failure)
                            #
                            # TODO
}
CLIENT[2] = CLIENT[1]
CLIENT[3] = {
    "HELLO": 0x01,          # HELLO <headers>
                            # -> SUCCESS - connection initialised
                            # -> FAILURE - init failed, disconnect (reconnect to retry)
                            #
                            # Initialisation is carried out once per connection, immediately
                            # after version negotiation. Before this, no other messages may
                            # validly be exchanged. INIT bundles with it two pieces of data:
                            # a user agent string and a map of auth information. More detail on
                            # on this can be found in the ConnectionSettings class below.

    "GOODBYE": 0x02,        # GOODBYE
                            # TODO

    "RESET": 0x0F,          # RESET
                            # -> SUCCESS - connection reset
                            # -> FAILURE - protocol error, disconnect
                            #
                            # A RESET is used to clear the connection state back to how it was
                            # immediately following initialisation. Specifically, any
                            # outstanding failure will be acknowledged, any result stream will
                            # be discarded and any transaction will be rolled back. This is
                            # used primarily by the connection pool.

    "RUN": 0x10,            # RUN <statement> <parameters>
                            # -> SUCCESS - statement accepted
                            # -> FAILURE - statement not accepted
                            # -> IGNORED - request ignored (due to prior failure)
                            #
                            # RUN is used to execute a Cypher statement and is paired with
                            # either PULL_ALL or DISCARD_ALL to retrieve or throw away the
                            # results respectively.
                            # TODO

    "BEGIN": 0x11,          # BEGIN
                            # TODO

    "COMMIT": 0x12,         # COMMIT
                            # TODO

    "ROLLBACK": 0x13,       # ROLLBACK
                            # TODO

    "DISCARD_ALL": 0x2F,    # DISCARD_ALL
                            # -> SUCCESS - result discarded
                            # -> FAILURE - no result to discard
                            # -> IGNORED - request ignored (due to prior failure)
                            #
                            # TODO

    "PULL_ALL": 0x3F,       # PULL_ALL
                            # .. [RECORD*] - zero or more RECORDS may be returned first
                            # -> SUCCESS - result complete
                            # -> FAILURE - no result to pull
                            # -> IGNORED - request ignored (due to prior failure)
                            #
                            # TODO
}
CLIENT[4] = {
    "HELLO": 0x01,          # HELLO <headers>
                            # -> SUCCESS - connection initialised
                            # -> FAILURE - init failed, disconnect (reconnect to retry)
                            #
                            # Initialisation is carried out once per connection, immediately
                            # after version negotiation. Before this, no other messages may
                            # validly be exchanged. INIT bundles with it two pieces of data:
                            # a user agent string and a map of auth information. More detail on
                            # on this can be found in the ConnectionSettings class below.

    "GOODBYE": 0x02,        # GOODBYE
                            # TODO

    "RESET": 0x0F,          # RESET
                            # -> SUCCESS - connection reset
                            # -> FAILURE - protocol error, disconnect
                            #
                            # A RESET is used to clear the connection state back to how it was
                            # immediately following initialisation. Specifically, any
                            # outstanding failure will be acknowledged, any result stream will
                            # be discarded and any transaction will be rolled back. This is
                            # used primarily by the connection pool.

    "RUN": 0x10,            # RUN <statement> <parameters>
                            # -> SUCCESS - statement accepted
                            # -> FAILURE - statement not accepted
                            # -> IGNORED - request ignored (due to prior failure)
                            #
                            # RUN is used to execute a Cypher statement and is paired with
                            # either PULL_ALL or DISCARD_ALL to retrieve or throw away the
                            # results respectively.
                            # TODO

    "BEGIN": 0x11,          # BEGIN
                            # TODO

    "COMMIT": 0x12,         # COMMIT
                            # TODO

    "ROLLBACK": 0x13,       # ROLLBACK
                            # TODO

    "DISCARD": 0x2F,        # DISCARD <n>
                            # -> SUCCESS - result discarded
                            # -> FAILURE - no result to discard
                            # -> IGNORED - request ignored (due to prior failure)
                            #
                            # TODO

    "PULL": 0x3F,           # PULL <n>
                            # .. [RECORD*] - zero or more RECORDS may be returned first
                            # -> SUCCESS - result complete
                            # -> FAILURE - no result to pull
                            # -> IGNORED - request ignored (due to prior failure)
                            #
                            # TODO
}
#
# The server responds with one or more of these for each request:
SERVER = [None] * (MAX_BOLT_VERSION + 1)
SERVER[1] = {
    "SUCCESS": 0x70,            # SUCCESS <metadata>
    "RECORD": 0x71,             # RECORD <value>
    "IGNORED": 0x7E,            # IGNORED <metadata>
    "FAILURE": 0x7F,            # FAILURE <metadata>
}
SERVER[4] = SERVER[3] = SERVER[2] = SERVER[1]


# TODO
log = getLogger("boltkit")


class Connection:
    """ The Connection wraps a socket through which protocol messages are sent
    and received. The socket is owned by this Connection instance...
    """

    # Maximum size of a single data chunk.
    max_chunk_size = 65535

    # The default address list to use if no addresses are specified.
    default_address_list = AddressList.parse(":7687 :17601 :17687")

    @classmethod
    def default_user_agent(cls):
        """ Return the default user agent string for a Connection.
        """
        package = "boltkit"
        version = "1.3.0"
        return "{}/{}".format(package, version)

    @classmethod
    def fix_bolt_versions(cls, bolt_versions):
        """ Using the requested Bolt versions, and falling back on the full
        list available, generate a tuple of exactly four Bolt protocol
        versions for use in version negotiation.
        """
        # Establish which protocol versions we want to attempt to use
        if not bolt_versions:
            bolt_versions = sorted([v for v, x in enumerate(CLIENT)
                                    if x is not None], reverse=True)
        # Raise an error if we're including any non-supported versions
        if any(v < 0 or v > MAX_BOLT_VERSION for v in bolt_versions):
            raise ValueError("This client does not support all "
                             "Bolt versions in %r" % bolt_versions)
        # Ensure we send exactly 4 versions, padding with zeroes if necessary
        return tuple(list(bolt_versions) + [0, 0, 0, 0])[:4]

    @classmethod
    def _open_to(cls, address, auth, user_agent, bolt_versions):
        """ Attempt to open a connection to a Bolt server, given a single
        socket address.
        """
        cx = None
        handshake_data = BOLT + b"".join(raw_pack(UINT_32, version)
                                         for version in bolt_versions)
        s = socket(family={2: AF_INET, 4: AF_INET6}[len(address)])
        try:
            s.connect(address)
            s.sendall(handshake_data)
            raw_bolt_version = s.recv(4)
            if raw_bolt_version:
                bolt_version, = raw_unpack(UINT_32, raw_bolt_version)
                if bolt_version > 0 and bolt_version in bolt_versions:
                    cx = cls(s, bolt_version, auth, user_agent)
                else:
                    log.error("Could not negotiate protocol version "
                              "(outcome=%d)", bolt_version)
            else:
                pass  # recv returned empty, peer closed connection
        finally:
            if not cx:
                s.close()
        return cx

    @classmethod
    def open(cls, *addresses, **config):
        """ Open a connection to a Bolt server. It is here that we create a
        low-level socket connection and carry out version negotiation.
        Following this (and assuming success) a Connection instance will be
        returned. This Connection takes ownership of the underlying socket
        and is subsequently responsible for managing its lifecycle.

        Args:
            addresses: Tuples of host and port, such as ("127.0.0.1", 7687).
            config: keyword settings:
                auth:
                user_agent:
                bolt_versions:
                timeout:

        Returns:
            A connection to the Bolt server.

        Raises:
            ProtocolError: if the protocol version could not be negotiated.
        """
        addresses = AddressList(addresses or cls.default_address_list)
        auth = config.get("auth")
        user_agent = config.get("user_agent")
        bolt_versions = config.get("bolt_versions")
        timeout = config.get("timeout", 0)
        addresses.resolve()
        t0 = timer()
        bolt_versions = cls.fix_bolt_versions(bolt_versions)
        log.info("Trying to open connection to «%s»", addresses)
        errors = set()
        again = True
        wait = 0.1
        while again:
            for address in addresses:
                try:
                    cx = cls._open_to(address, auth, user_agent, bolt_versions)
                except OSError as e:
                    errors.add(" ".join(map(str, e.args)))
                else:
                    if cx:
                        return cx
            again = timer() - t0 < (timeout or 0)
            if again:
                sleep(wait)
                wait *= 2
        log.error("Could not open connection to «%s» (%r)",
                  addresses, errors)
        raise OSError("Could not open connection")

    def __init__(self, s, bolt_version, auth, user_agent=None):
        self.socket = s
        self.address = AddressList([self.socket.getpeername()])
        self.bolt_version = bolt_version
        log.info("Opened connection to «%s» using Bolt v%d",
                 self.address, self.bolt_version)
        self.requests = []
        self.responses = []
        try:
            user, password = auth
        except (TypeError, ValueError):
            user, password = "neo4j", ""
        if user_agent is None:
            user_agent = self.default_user_agent()
        if bolt_version >= 3:
            args = {
                "scheme": "basic",
                "principal": user,
                "credentials": password,
                "user_agent": user_agent,
            }
            log.debug("C: HELLO %r" % dict(args, credentials="..."))
            request = Structure(CLIENT[self.bolt_version]["HELLO"], args)
        else:
            auth_token = {
                "scheme": "basic",
                "principal": user,
                "credentials": password,
            }
            log.debug("C: INIT %r %r", user_agent, dict(auth_token, credentials="..."))
            request = Structure(CLIENT[self.bolt_version]["INIT"], user_agent, auth_token)
        self.requests.append(request)
        response = Response(self.bolt_version)
        self.responses.append(response)
        self.send_all()
        self.fetch_all()
        self.server_agent = response.metadata["server"]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        log.info("Closing connection to «%s»", self.address)
        self.socket.close()

    def reset(self):
        log.debug("C: RESET")
        self.requests.append(Structure(CLIENT[self.bolt_version]["RESET"]))
        self.send_all()
        response = Response(self.bolt_version)
        self.responses.append(response)
        while not response.complete():
            self.fetch_one()

    def run(self, statement, parameters=None, metadata=None):
        parameters = parameters or {}
        metadata = metadata or {}
        if self.bolt_version >= 3:
            log.debug("C: RUN %r %r %r", statement, parameters, metadata)
            run = Structure(CLIENT[self.bolt_version]["RUN"], statement, parameters, metadata)
        elif metadata:
            raise ProtocolError("RUN metadata is not available in Bolt v%d" % self.bolt_version)
        else:
            log.debug("C: RUN %r %r", statement, parameters)
            run = Structure(CLIENT[self.bolt_version]["RUN"], statement, parameters)
        self.requests.append(run)
        response = QueryResponse(self.bolt_version)
        self.responses.append(response)
        return response

    def discard(self, n, qid):
        """ Enqueue a DISCARD message.

        :param n: number of records to discard (-1 means all)
        :param qid: the query for which to discard records (-1 means the query
                    immediately preceding)
        :return: :class:`.QueryResponse` object
        """
        v = self.bolt_version
        if v >= 4:
            args = {"n": n}
            if qid >= 0:
                args["qid"] = qid
            log.debug("C: DISCARD %r", args)
            self.requests.append(Structure(CLIENT[v]["DISCARD"], args))
        elif n >= 0 or qid >= 0:
            raise ProtocolError("Reactive DISCARD is not available in "
                                "Bolt v%d" % v)
        else:
            log.debug("C: DISCARD_ALL")
            self.requests.append(Structure(CLIENT[v]["DISCARD_ALL"]))
        response = QueryResponse(v)
        self.responses.append(response)
        return response

    def pull(self, n, qid, records):
        """ Enqueue a PULL message.

        :param n: number of records to pull (-1 means all)
        :param qid: the query for which to pull records (-1 means the query
                    immediately preceding)
        :param records: list-like container into which records may be appended
        :return: :class:`.QueryResponse` object
        """
        v = self.bolt_version
        if v >= 4:
            args = {"n": n}
            if qid >= 0:
                args["qid"] = qid
            log.debug("C: PULL %r", args)
            self.requests.append(Structure(CLIENT[v]["PULL"], args))
        elif n >= 0 or qid >= 0:
            raise ProtocolError("Reactive PULL is not available in "
                                "Bolt v%d" % v)
        else:
            log.debug("C: PULL_ALL")
            self.requests.append(Structure(CLIENT[v]["PULL_ALL"]))
        response = QueryResponse(v, records)
        self.responses.append(response)
        return response

    def begin(self, metadata=None):
        metadata = metadata or {}
        if self.bolt_version >= 3:
            log.debug("C: BEGIN %r", metadata)
            self.requests.append(Structure(CLIENT[self.bolt_version]["BEGIN"], metadata))
        else:
            raise ProtocolError("BEGIN is not available in Bolt v%d" % self.bolt_version)
        response = QueryResponse(self.bolt_version)
        self.responses.append(response)
        return response

    def commit(self):
        if self.bolt_version >= 3:
            log.debug("C: COMMIT")
            self.requests.append(Structure(CLIENT[self.bolt_version]["COMMIT"]))
        else:
            raise ProtocolError("COMMIT is not available in Bolt v%d" % self.bolt_version)
        response = QueryResponse(self.bolt_version)
        self.responses.append(response)
        return response

    def rollback(self):
        if self.bolt_version >= 3:
            log.debug("C: ROLLBACK")
            self.requests.append(Structure(CLIENT[self.bolt_version]["ROLLBACK"]))
        else:
            raise ProtocolError("ROLLBACK is not available in Bolt v%d" % self.bolt_version)
        response = QueryResponse(self.bolt_version)
        self.responses.append(response)
        return response

    def send_all(self):
        """ Send all pending request messages to the server.
        """
        if not self.requests:
            return
        data = []
        while self.requests:
            request = self.requests.pop(0)
            request_data = pack(request)
            for offset in range(0, len(request_data), self.max_chunk_size):
                end = offset + self.max_chunk_size
                chunk = request_data[offset:end]
                data.append(raw_pack(UINT_16, len(chunk)))
                data.append(chunk)
            data.append(raw_pack(UINT_16, 0))
        self.socket.sendall(b"".join(data))

    def fetch_one(self):
        """ Receive exactly one response message from the server. This method blocks until either a
        message arrives or the connection is terminated.
        """

        # Receive chunks of data until chunk_size == 0
        data = []
        chunk_size = -1
        while chunk_size != 0:
            chunk_size, = raw_unpack(UINT_16, self.socket.recv(2))
            if chunk_size > 0:
                data.append(self.socket.recv(chunk_size))
        message = unpack(b"".join(data))

        # Handle message
        response = self.responses[0]
        try:
            response.on_message(message.tag, *message.fields)
        except Failure as failure:
            if isinstance(failure.response, QueryResponse):
                self.reset()
            else:
                self.close()
            raise
        finally:
            if response.complete():
                self.responses.pop(0)

    def fetch_summary(self):
        """ Fetch all messages up to and including the next summary message.
        """
        response = self.responses[0]
        while not response.complete():
            self.fetch_one()

    def fetch_all(self):
        """ Fetch all messages from all outstanding responses.
        """
        while self.responses:
            self.fetch_summary()


class Response:
    # Basic request that expects SUCCESS or FAILURE back, e.g. RESET

    def __init__(self, bolt_version):
        self.bolt_version = bolt_version
        self.metadata = None

    def complete(self):
        return self.metadata is not None

    def on_success(self, data):
        log.debug("S: SUCCESS %r", data)
        self.metadata = data

    def on_failure(self, data):
        log.debug("S: FAILURE %r", data)
        self.metadata = data
        raise Failure(self)

    def on_message(self, tag, data=None):
        if tag == SERVER[self.bolt_version]["SUCCESS"]:
            self.on_success(data)
        elif tag == SERVER[self.bolt_version]["FAILURE"]:
            self.on_failure(data)
        else:
            raise ProtocolError("Unexpected summary message with tag %02X received" % tag)


class QueryResponse(Response):
    # Can also be IGNORED (RUN, DISCARD_ALL)

    def __init__(self, bolt_version, records=None):
        super(QueryResponse, self).__init__(bolt_version)
        self.ignored = False
        self.records = records

    def on_ignored(self, data):
        log.debug("S: IGNORED %r", data)
        self.ignored = True
        self.metadata = data

    def on_record(self, data):
        log.debug("S: RECORD %r", data)
        if self.records is not None:
            self.records.append(data)

    def on_message(self, tag, data=None):
        if tag == SERVER[self.bolt_version]["RECORD"]:
            self.on_record(data)
        elif tag == SERVER[self.bolt_version]["IGNORED"]:
            self.on_ignored(data)
        else:
            super(QueryResponse, self).on_message(tag, data)


class Failure(Exception):

    def __init__(self, response):
        super(Failure, self).__init__(response.metadata["message"])
        self.response = response
        self.code = response.metadata["code"]


class ProtocolError(Exception):

    pass
