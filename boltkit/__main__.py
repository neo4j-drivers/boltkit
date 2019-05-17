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

from boltkit.addressing import Address, AddressList
from boltkit.auth import AuthParamType, Auth
from boltkit.client import Connection
from boltkit.dist import Distributor
from boltkit.server import Neo4jService
from boltkit.server.proxy import ProxyServer
from boltkit.server.stub import StubServer
from boltkit.watcher import watch


class AddressParamType(click.ParamType):

    name = "addr"

    def __init__(self, default_host=None, default_port=None):
        self.default_host = default_host
        self.default_port = default_port

    def convert(self, value, param, ctx):
        return Address.parse(value, self.default_host, self.default_port)

    def __repr__(self):
        return 'HOST:PORT'


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


@bolt.command(help="""\
Run a Bolt client.
""")
@click.option("-a", "--auth", type=AuthParamType(), envvar="NEO4J_AUTH")
@click.option("-b", "--bolt-version", default=0, type=int)
@click.option("-s", "--server-addr", type=AddressListParamType(), envvar="BOLT_SERVER_ADDR")
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
                cx.pull(-1, -1, records)
            if transaction:
                cx.commit()
            cx.send_all()
            cx.fetch_all()
            for record in records:
                click.echo("\t".join(map(str, record)))
    except Exception as e:
        click.echo(" ".join(map(str, e.args)))
        exit(1)


@bolt.command(help="""\
Run a Bolt stub server.

The stub server process listens for an incoming client connection and will 
attempt to play through a pre-scripted exchange with that client. Any deviation
from that script will result in a non-zero exit code. This utility is primarily
useful for Bolt client integration testing.
""")
@click.option("-l", "--listen-addr", type=AddressParamType(),
              envvar="BOLT_LISTEN_ADDR",
              help="The address on which to listen for incoming connections "
                   "in INTERFACE:PORT format, where INTERFACE may be omitted "
                   "for 'localhost'. If completely omitted, this defaults to "
                   "':17687'. The BOLT_LISTEN_ADDR environment variable may "
                   "be used as an alternative to this option.")
@click.option("-t", "--timeout", type=float,
              help="The number of seconds for which the stub server will wait "
                   "for an incoming connection before automatically "
                   "terminating. If unspecified, the server will wait "
                   "indefinitely.")
@click.option("-v", "--verbose", count=True, callback=watch_log,
              expose_value=False, is_eager=True,
              help="Show more detail about the client-server exchange.")
@click.argument("script")
def stub(script, listen_addr, timeout):
    stub_server = StubServer(script, listen_addr, timeout=timeout)
    try:
        stub_server.start()
        stub_server.join()
    except KeyboardInterrupt:
        exit(130)
    except Exception as e:
        click.echo(" ".join(map(str, e.args)), err=True)
        exit(1)
    finally:
        exit(stub_server.exit_code)


@bolt.command(help="""\
Run a Bolt proxy server.
""")
@click.option("-l", "--listen-addr", type=AddressParamType(), envvar="BOLT_LISTEN_ADDR")
@click.option("-s", "--server-addr", type=AddressListParamType(), envvar="BOLT_SERVER_ADDR")
@click.option("-v", "--verbose", count=True, callback=watch_log, expose_value=False, is_eager=True)
def proxy(server_addr, listen_addr):
    proxy_server = ProxyServer(server_addr, listen_addr)
    proxy_server.start()


@bolt.command(help="List available Neo4j releases")
def dist():
    try:
        distributor = Distributor()
        for name, r in distributor.releases.items():
            if name == r.name.upper():
                click.echo(r.name)
    except KeyboardInterrupt:
        exit(130)
    except Exception as e:
        click.echo(" ".join(map(str, e.args)), err=True)
        exit(1)


@bolt.command(help="""\
Download Neo4j.
""")
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
    except KeyboardInterrupt:
        exit(130)
    except Exception as e:
        click.echo(" ".join(map(str, e.args)), err=True)
        exit(1)


@bolt.command(context_settings={"ignore_unknown_options": True}, help="""\
Run a Neo4j cluster or standalone server in one or more local Docker 
containers.

If an additional COMMAND is supplied, this will be executed after startup, 
with a shutdown occurring immediately afterwards. If no COMMAND is supplied,
the service will remain available until manually shutdown by Ctrl+C.

A couple of environment variables will also be made available to any COMMAND
passed. These are:

\b
- BOLT_SERVER_ADDR
- NEO4J_AUTH
""")
@click.option("-a", "--auth", type=AuthParamType(), envvar="NEO4J_AUTH",
              help="Credentials with which to bootstrap the service. These "
                   "must be specified as a 'user:password' pair and may "
                   "alternatively be supplied via the NEO4J_AUTH environment "
                   "variable. These credentials will also be exported to any "
                   "COMMAND executed during the service run.")
@click.option("-B", "--bolt-port", type=int,
              help="A port number (standalone) or base port number (cluster) "
                   "for Bolt traffic.")
@click.option("-c", "--n-cores", type=int,
              help="If specified, a cluster with this many cores will be "
                   "created. If omitted, a standalone service will be created "
                   "instead. See also -r for specifying the number of read "
                   "replicas.")
@click.option("-H", "--http-port", type=int,
              help="A port number (standalone) or base port number (cluster) "
                   "for HTTP traffic.")
@click.option("-i", "--image",
              help="The Docker image tag to use for building containers. The "
                   "repository can also be included, but will default to "
                   "'neo4j'. Note that a Neo4j Enterprise Edition image is "
                   "required for building clusters.")
@click.option("-n", "--name",
              help="A Docker network name to which all servers will be "
                   "attached. If omitted, an auto-generated name will be "
                   "used.")
@click.option("-r", "--n-replicas", type=int,
              help="The number of read replicas to include within the "
                   "cluster. This option will only take effect if -c is also "
                   "used.")
@click.option("-v", "--verbose", count=True, callback=watch_log,
              expose_value=False, is_eager=True,
              help="Show more detail about the startup and shutdown process.")
@click.argument("command", nargs=-1, type=click.UNPROCESSED)
def server(command, name, **parameters):
    try:
        with Neo4jService(name, **parameters) as neo4j:
            addr = AddressList(chain(*(r.address for r in neo4j.routers)))
            auth = "{}:{}".format(neo4j.auth.user, neo4j.auth.password)
            if command:
                run(" ".join(map(shlex_quote, command)), shell=True, env={
                    "BOLT_SERVER_ADDR": str(addr),
                    "NEO4J_AUTH": auth,
                })
            else:
                click.echo("BOLT_SERVER_ADDR='{}'".format(addr))
                click.echo("NEO4J_AUTH='{}'".format(auth))
                click.echo("Press Ctrl+C to exit")
                while True:
                    sleep(0.1)
    except KeyboardInterrupt:
        exit(130)
    except Exception as e:
        click.echo(" ".join(map(str, e.args)), err=True)
        exit(1)


if __name__ == "__main__":
    bolt()
