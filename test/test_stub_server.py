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


from pytest import mark, raises

from boltkit.client import Connection
from boltkit.server.scripting import ScriptMismatch
from boltkit.server.stub import BoltStubService


def script(*paths):
    from importlib import import_module
    from os.path import dirname, join
    return join(dirname(import_module("test").__file__), "scripts", *paths)


@mark.asyncio
async def test_v1():

    async with BoltStubService.load(script("v1", "return_1_as_x.bolt")) as service:

        # Given
        with Connection.open(*service.addresses, auth=service.auth, bolt_versions=[(1, 0)]) as cx:

            # When
            records = []
            cx.run("RETURN $x", {"x": 1})
            cx.pull(-1, -1, records)
            cx.send_all()
            cx.fetch_all()

            # Then
            assert records == [[1]]
            assert cx.bolt_version == (1, 0)


@mark.asyncio
async def test_v2():

    async with BoltStubService.load(script("v2", "return_1_as_x.bolt")) as service:

        # Given
        with Connection.open(*service.addresses, auth=service.auth) as cx:

            # When
            records = []
            cx.run("RETURN $x", {"x": 1})
            cx.pull(-1, -1, records)
            cx.send_all()
            cx.fetch_all()

            # Then
            assert records == [[1]]
            assert cx.bolt_version == (2, 0)


@mark.asyncio
async def test_v3():

    async with BoltStubService.load(script("v3", "return_1_as_x.bolt")) as service:

        # Given
        with Connection.open(*service.addresses, auth=service.auth) as cx:

            # When
            records = []
            cx.run("RETURN $x", {"x": 1})
            cx.pull(-1, -1, records)
            cx.send_all()
            cx.fetch_all()

            # Then
            assert records == [[1]]
            assert cx.bolt_version == (3, 0)


@mark.asyncio
async def test_v3_mismatch():

    with raises(ScriptMismatch):

        async with BoltStubService.load(script("v3", "return_1_as_x.bolt")) as service:

            # Given
            with Connection.open(*service.addresses, auth=service.auth) as cx:

                # When
                records = []
                cx.run("RETURN $x", {"x": 2})
                cx.send_all()
                cx.fetch_all()


@mark.asyncio
async def test_v4x0():

    async with BoltStubService.load(script("v4.0", "return_1_as_x.bolt")) as service:

        # Given
        with Connection.open(*service.addresses, auth=service.auth) as cx:

            # When
            records = []
            cx.run("RETURN $x", {"x": 1})
            cx.pull(-1, -1, records)
            cx.send_all()
            cx.fetch_all()

            # Then
            assert records == [[1]]
            assert cx.bolt_version == (4, 0)


@mark.asyncio
async def test_v4x0_explicit():

    async with BoltStubService.load(script("v4.0", "return_1_as_x_explicit.bolt")) as service:

        # Given
        with Connection.open(*service.addresses, auth=service.auth) as cx:

            # When
            records = []
            cx.begin()
            cx.run("RETURN $x", {"x": 1})
            cx.pull(-1, -1, records)
            cx.commit()
            cx.send_all()
            cx.fetch_all()


@mark.asyncio
async def test_v4x1():

    async with BoltStubService.load(script("v4.1", "return_1_as_x.bolt")) as service:

        # Given
        with Connection.open(*service.addresses, auth=service.auth) as cx:

            # When
            records = []
            cx.run("RETURN $x", {"x": 1})
            cx.pull(-1, -1, records)
            cx.send_all()
            cx.fetch_all()

            # Then
            assert records == [[1]]
            assert cx.bolt_version == (4, 1)

            # Then
            assert records == [[1]]
            assert cx.bolt_version == (4, 1)
