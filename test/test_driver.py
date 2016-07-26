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

from os.path import dirname, join as path_join
from unittest import TestCase

from boltkit.driver import Driver
from boltkit.server import StubServer


class DriverTestCase(TestCase):

    port = 7687
    address = ("127.0.0.1", port)
    script = path_join(dirname(__file__), "scripts", "connect.script")
    timeout = 5

    def setUp(self):
        self.server = StubServer(self.address, self.script, self.timeout)
        self.server.start()

    def tearDown(self):
        self.server.stop()

    def test_driver_can_create_session_with_full_uri(self):
        # Given
        driver = Driver("bolt://127.0.0.1:7687", user="neo4j", password="password")

        # When
        session = driver.session()

        # Then
        address = session.connection.socket.getpeername()
        assert address == ("127.0.0.1", 7687), "Session not connected to expected address"

    def test_driver_can_create_session_with_default_port(self):
        # Given
        driver = Driver("bolt://127.0.0.1", user="neo4j", password="password")

        # When
        session = driver.session()

        # Then
        address = session.connection.socket.getpeername()
        assert address == ("127.0.0.1", 7687), "Session not connected to expected address"
