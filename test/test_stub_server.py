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


from unittest import TestCase

from boltkit.connector import connect
from boltkit.server import stub_test


class ReturnOneTestCase(TestCase):

    @stub_test("scripts/v1/return_1.bolt")
    def test_v1(self, server):

        # Given
        with connect(server.address, **server.settings) as cx:

            # When
            records = []
            cx.run("RETURN $x", {"x": 1})
            cx.pull(-1, records)
            cx.send_all()
            cx.fetch_all()

            # Then
            self.assertEqual(records, [[1]])
            self.assertEqual(cx.bolt_version, 1)

    @stub_test("scripts/v2/return_1.bolt")
    def test_v2(self, server):

        # Given
        with connect(server.address, **server.settings) as cx:

            # When
            records = []
            cx.run("RETURN $x", {"x": 1})
            cx.pull(-1, records)
            cx.send_all()
            cx.fetch_all()

            # Then
            self.assertEqual(records, [[1]])
            self.assertEqual(cx.bolt_version, 2)

    @stub_test("scripts/v3/return_1.bolt")
    def test_v3(self, server):

        # Given
        with connect(server.address, **server.settings) as cx:

            # When
            records = []
            cx.run("RETURN $x", {"x": 1})
            cx.pull(-1, records)
            cx.send_all()
            cx.fetch_all()

            # Then
            self.assertEqual(records, [[1]])
            self.assertEqual(cx.bolt_version, 3)

    @stub_test("scripts/v4/return_1.bolt")
    def test_v4(self, server):

        # Given
        with connect(server.address, **server.settings) as cx:

            # When
            records = []
            cx.run("RETURN $x", {"x": 1})
            cx.pull(-1, records)
            cx.send_all()
            cx.fetch_all()

            # Then
            self.assertEqual(records, [[1]])
            self.assertEqual(cx.bolt_version, 4)

    @stub_test("scripts/v4/return_1_explicit.bolt")
    def test_v4_explicit(self, server):

        # Given
        with connect(server.address, **server.settings) as cx:

            # When
            records = []
            cx.begin()
            cx.run("RETURN $x", {"x": 1})
            cx.pull(-1, records)
            cx.commit()
            cx.send_all()
            cx.fetch_all()

            # Then
            self.assertEqual(records, [[1]])
            self.assertEqual(cx.bolt_version, 4)
