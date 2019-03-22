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


import click

from .client import connect, ProtocolError
from .server import ProxyServer, StubServer
from .watcher import watch


@click.group()
def bolt():
    pass


@bolt.command()
@click.option("-b", "--bolt-version", default=0, type=int)
@click.option("-h", "--host", default="localhost", show_default=True)
@click.option("-p", "--port", default="7687", show_default=True, type=int)
@click.option("-t", "--transaction", is_flag=True)
@click.option("-u", "--user", default="neo4j", show_default=True)
@click.option("-v", "--verbose", is_flag=True)
@click.password_option("-w", "--password", confirmation_prompt=False)
@click.argument("cypher", nargs=-1)
def client(cypher, host, port, user, password, transaction, verbose, bolt_version):
    if verbose:
        watch("bolt.client")
    address = (host, port)
    if bolt_version:
        bolt_versions = [bolt_version]
    else:
        bolt_versions = None
    try:
        with connect(address, user=user, password=password, bolt_versions=bolt_versions) as cx:
            records = []
            if transaction:
                cx.begin()
            for statement in cypher:
                cx.run(statement, {})
                cx.pull(-1, records)
            if transaction:
                cx.commit()
            cx.send_all()
            cx.fetch_all()
            for record in records:
                print("\t".join(map(str, record)))
    except ProtocolError as e:
        print("Protocol Error: %s" % e.args[0])
        exit(1)


@bolt.command()
@click.option("-H", "--bind-host", default="localhost", show_default=True)
@click.option("-P", "--bind-port", default=17687, type=int, show_default=True)
@click.option("-v", "--verbose", is_flag=True)
@click.argument("script")
def stub_server(bind_host, bind_port, script, verbose):
    if verbose:
        watch("bolt.server")
    server = StubServer((bind_host, bind_port), script)
    server.start()
    try:
        while server.is_alive():
            pass
    except KeyboardInterrupt:
        pass
    exit(0 if not server.script else 1)


@bolt.command()
@click.option("-H", "--bind-host", default="localhost", show_default=True)
@click.option("-P", "--bind-port", default=17687, type=int, show_default=True)
@click.option("-h", "--server-host", default="localhost", show_default=True)
@click.option("-p", "--server-port", default=7687, type=int, show_default=True)
def proxy_server(bind_host, bind_port, server_host, server_port):
    server = ProxyServer((bind_host, bind_port), (server_host, server_port))
    server.start()


if __name__ == "__main__":
    bolt()
