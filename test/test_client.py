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


from unittest import TestCase

from boltkit.client import pack
from boltkit.server.bytetools import h


class PackerTestCase(TestCase):

    def test_integer(self):
        self.assertEqual(h(pack(1)), '01')
        self.assertEqual(h(pack(1234)), 'C9:04:D2')

    def test_float(self):
        self.assertEqual(h(pack(6.283185307179586)), 'C1:40:19:21:FB:54:44:2D:18')

    def test_boolean(self):
        self.assertEqual(h(pack(False)), 'C2')

    def test_string(self):
        self.assertEqual(h(pack("Übergröße")), '8C:C3:9C:62:65:72:67:72:C3:B6:C3:9F:65')

    def test_mixed_list(self):
        self.assertEqual(h(pack([1, True, 3.14, "fünf"])),
                         '94:01:C3:C1:40:09:1E:B8:51:EB:85:1F:85:66:C3:BC:6E:66')
