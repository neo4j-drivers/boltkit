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


from socket import getaddrinfo, SOCK_STREAM

from click import ParamType


class AddressList(list):
    """ A list of socket addresses, each as a tuple of the format expected by
    the built-in `socket.connect` method.
    """

    @classmethod
    def parse(cls, s, default_host=None, default_port=None):
        """ Parse a string containing one or more socket addresses, each
        separated by whitespace.
        """
        if isinstance(s, str):
            parsed = []
            for addr in s.split():
                if addr.startswith("["):
                    # IPv6
                    host, _, port = addr[1:].rpartition("]")
                    parsed.append((host, port.lstrip(":"), 0, 0))
                else:
                    # IPv4
                    host, _, port = addr.partition(":")
                    parsed.append((host, port))
            return cls(parsed, default_host, default_port)
        else:
            raise TypeError("AddressList.parse requires a string argument")

    def __init__(self, iterable=None, default_host=None, default_port=None):
        items = list(iterable or ())
        for item in items:
            if not isinstance(item, tuple):
                raise TypeError("Object {!r} is not a valid address "
                                "(tuple expected)".format(item))
        super().__init__(items)
        self.default_host = default_host
        self.default_port = default_port

    def __str__(self):
        s = []
        for address in iter(self):
            if len(address) == 4:
                s.append("[{}]:{}".format(*address))
            else:
                s.append("{}:{}".format(*address))
        return " ".join(s)

    def __repr__(self):
        return "{}({!r})".format(self.__class__.__name__, list(self))

    def resolve(self, family=0):
        """ Resolve all addresses into one or more resolved address tuples.
        Each host name will resolve into one or more IP addresses, limited by
        the given address `family` (if any). Each port value (either integer
        or string) will resolve into an integer port value (e.g. 'http' will
        resolve to 80).

            >>> a = AddressList([("localhost", "http")])
            >>> a.resolve()
            >>> a
            AddressList([('::1', 80, 0, 0), ('127.0.0.1', 80)])

        """
        resolved = []
        for address in iter(self):
            host = address[0] or self.default_host or "localhost"
            port = address[1] or self.default_port or 0
            for _, _, _, _, addr in getaddrinfo(host, port, family,
                                                SOCK_STREAM):
                if addr not in resolved:
                    resolved.append(addr)
        self[:] = resolved
