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


from itertools import chain
from logging import INFO, DEBUG
from shlex import quote as shlex_quote
from subprocess import run
from time import sleep

import click

from .addressing import AddressList
from .client import Connection
from .containers import Neo4jService
from .dist import Distributor
from .auth import AuthParamType, Auth
from .server import ProxyServer, StubServer
from .watcher import watch


class AddressListParamType(click.ParamType):

    name = "addr"

    def __init__(self, default_host=None, default_port=None):
        self.default_host = default_host
        self.default_port = default_port

    def convert(self, value, param, ctx):
        return AddressList.parse(value, self.default_host, self.default_port)

    def __repr__(self):
        return 'HOST:PORT [HOST:PORT...]'


def watch_log(ctx, param, value):
    if value:
        watch("boltkit", DEBUG if value >= 2 else INFO)


@click.group()
def bolt():
    pass


@bolt.command(help="Run a Bolt client")
@click.option("-a", "--auth", type=AuthParamType(), envvar="NEO4J_AUTH")
@click.option("-b", "--bolt-version", default=0, type=int)
@click.option("-s", "--server-addr", type=AddressListParamType(), envvar="NEO4J_SERVER_ADDR")
@click.option("-t", "--transaction", is_flag=True)
@click.option("-v", "--verbose", count=True, callback=watch_log, expose_value=False, is_eager=True)
@click.argument("cypher", nargs=-1)
def client(cypher, server_addr, auth, transaction, bolt_version):
    if auth is None:
        auth = Auth(click.prompt("User", default="neo4j"),
                    click.prompt("Password", hide_input=True))
    if bolt_version:
        bolt_versions = [bolt_version]
    else:
        bolt_versions = None
    try:
        with Connection.open(*server_addr or (), auth=auth, bolt_versions=bolt_versions) as cx:
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
                click.echo("\t".join(map(str, record)))
    except Exception as e:
        click.echo(" ".join(map(str, e.args)))
        exit(1)


@bolt.command(help="Run a Bolt stub server")
@click.option("-H", "--bind-host", default="localhost", show_default=True)
@click.option("-P", "--bind-port", default=17687, type=int, show_default=True)
@click.option("-v", "--verbose", count=True, callback=watch_log, expose_value=False, is_eager=True)
@click.argument("script")
def stub(bind_host, bind_port, script):
    stub_server = StubServer((bind_host, bind_port), script)
    stub_server.start()
    try:
        while stub_server.is_alive():
            pass
    except KeyboardInterrupt:
        pass
    exit(0 if not stub_server.script else 1)


@bolt.command(help="Run a Bolt proxy server")
@click.option("-H", "--bind-host", default="localhost", show_default=True)
@click.option("-P", "--bind-port", default=17687, type=int, show_default=True)
@click.option("-s", "--server-addr", type=AddressListParamType(), envvar="NEO4J_SERVER_ADDR")
@click.option("-v", "--verbose", count=True, callback=watch_log, expose_value=False, is_eager=True)
def proxy(bind_host, bind_port, server_addr):
    proxy_server = ProxyServer((bind_host, bind_port), server_addr)
    proxy_server.start()


@bolt.command(help="List available Neo4j releases")
def dist():
    try:
        distributor = Distributor()
        for name, r in distributor.releases.items():
            if name == r.name.upper():
                click.echo(r.name)
    except Exception as e:
        click.echo(" ".join(e.args))
        exit(1)


@bolt.command(help="Download Neo4j")
@click.option("-e", "--enterprise", is_flag=True)
@click.option("-s", "--s3", is_flag=True)
@click.option("-t", "--teamcity", is_flag=True)
@click.option("-v", "--verbose", count=True, callback=watch_log, expose_value=False, is_eager=True)
@click.option("-w", "--windows", is_flag=True)
@click.argument("version")
def get(version, enterprise, s3, teamcity, windows):
    try:
        distributor = Distributor()
        edition = "enterprise" if enterprise else "community"
        if windows:
            package_format = "windows.zip"
        else:
            package_format = "unix.tar.gz"
        if s3:
            distributor.download_from_s3(edition, version, package_format)
        elif teamcity:
            distributor.download_from_teamcity(edition, version, package_format)
        else:
            distributor.download(edition, version, package_format)
    except Exception as e:
        click.echo(" ".join(map(str, e.args)))
        exit(1)


@bolt.command(context_settings=dict(
    ignore_unknown_options=True,
), help="""\
Run a Neo4j cluster or standalone server.
""")
@click.option("-a", "--auth", type=AuthParamType(), envvar="NEO4J_AUTH")
@click.option("-B", "--bolt-port", type=int)
@click.option("-c", "--n-cores", type=int)
@click.option("-H", "--http-port", type=int)
@click.option("-i", "--image")
@click.option("-n", "--name")
@click.option("-r", "--n-replicas", type=int)
@click.option("-v", "--verbose", count=True, callback=watch_log, expose_value=False, is_eager=True)
@click.argument("command", nargs=-1, type=click.UNPROCESSED)
def server(command, name, **parameters):
    try:
        with Neo4jService(name, **parameters) as neo4j:
            addr = AddressList(chain(*(r.address for r in neo4j.routers)))
            auth = "{}:{}".format(neo4j.auth.user, neo4j.auth.password)
            if command:
                run(" ".join(map(shlex_quote, command)), shell=True, env={
                    "NEO4J_SERVER_ADDR": str(addr),
                    "NEO4J_AUTH": auth,
                })
            else:
                click.echo("NEO4J_SERVER_ADDR='{}'".format(addr))
                click.echo("NEO4J_AUTH='{}'".format(auth))
                click.echo("Press Ctrl+C to exit")
                while True:
                    sleep(0.1)
    except KeyboardInterrupt:
        exit(0)
    except Exception as e:
        click.echo(" ".join(map(str, e.args)))
        exit(1)


if __name__ == "__main__":
    bolt()
