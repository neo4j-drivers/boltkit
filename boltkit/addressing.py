#!/usr/bin/env python
# coding: utf-8

# Copyright (c) 2002-2020 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
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


from socket import getaddrinfo, getservbyname, SOCK_STREAM, AF_INET, AF_INET6


class Address(tuple):

    @classmethod
    def parse(cls, s, default_host=None, default_port=None):
        if isinstance(s, str):
            if s.startswith("["):
                # IPv6
                host, _, port = s[1:].rpartition("]")
                return cls((host or default_host or "localhost",
                            port.lstrip(":") or default_port or 0,
                            0, 0))
            else:
                # IPv4
                host, _, port = s.partition(":")
                return cls((host or default_host or "localhost",
                            port or default_port or 0))
        else:
            raise TypeError("Address.parse requires a string argument")

    def __new__(cls, iterable):
        n_parts = len(iterable)
        if n_parts == 2:
            inst = tuple.__new__(cls, iterable)
            inst.family = AF_INET
        elif n_parts == 4:
            inst = tuple.__new__(cls, iterable)
            inst.family = AF_INET6
        else:
            raise ValueError("Addresses must consist of either "
                             "two parts (IPv4) or four parts (IPv6)")
        return inst

    def __str__(self):
        if self.family == AF_INET6:
            return "[{}]:{}".format(*self)
        else:
            return "{}:{}".format(*self)

    def __repr__(self):
        return "{}({!r})".format(self.__class__.__name__, tuple(self))

    @property
    def host(self):
        return self[0]

    @property
    def port(self):
        return self[1]

    @property
    def port_number(self):
        try:
            return getservbyname(self[1])
        except (OSError, TypeError):
            # OSError: service/proto not found
            # TypeError: getservbyname() argument 1 must be str, not X
            try:
                return int(self[1])
            except (TypeError, ValueError) as e:
                raise type(e)("Unknown port value %r" % self[1])


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
            return cls([Address.parse(a, default_host, default_port)
                        for a in s.split()])
        else:
            raise TypeError("AddressList.parse requires a string argument")

    def __init__(self, iterable=None):
        items = list(iterable or ())
        for item in items:
            if not isinstance(item, tuple):
                raise TypeError("Object {!r} is not a valid address "
                                "(tuple expected)".format(item))
        super().__init__(items)

    def __str__(self):
        return " ".join(str(Address(_)) for _ in self)

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
            host = address[0]
            port = address[1]
            for _, _, _, _, addr in getaddrinfo(host, port, family,
                                                SOCK_STREAM):
                if addr not in resolved:
                    resolved.append(addr)
        self[:] = resolved
