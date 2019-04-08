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


from shlex import quote as shlex_quote
from subprocess import run
from time import sleep
from uuid import uuid4

import click

from .client import Connection
from .containers import Neo4jService
from .dist import Distributor
from .server import ProxyServer, StubServer
from .watcher import watch


@click.group()
def bolt():
    pass


@bolt.command(help="Run a Bolt client")
@click.option("-b", "--bolt-version", default=0, type=int)
@click.option("-a", "--addresses", multiple=True)
@click.option("-t", "--transaction", is_flag=True)
@click.option("-u", "--user", default="neo4j", show_default=True)
@click.option("-v", "--verbose", is_flag=True)
@click.password_option("-p", "--password", confirmation_prompt=False)
@click.argument("cypher", nargs=-1)
def client(cypher, addresses, user, password, transaction, verbose, bolt_version):
    if verbose:
        watch("bolt.client")
    if bolt_version:
        bolt_versions = [bolt_version]
    else:
        bolt_versions = None
    try:
        with Connection.open(*addresses, user=user, password=password, bolt_versions=bolt_versions) as cx:
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
        click.echo(" ".join(e.args))
        exit(1)


@bolt.command(help="Run a Bolt stub server")
@click.option("-H", "--bind-host", default="localhost", show_default=True)
@click.option("-P", "--bind-port", default=17687, type=int, show_default=True)
@click.option("-v", "--verbose", is_flag=True)
@click.argument("script")
def stub(bind_host, bind_port, script, verbose):
    if verbose:
        watch("bolt.server")
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
@click.option("-h", "--server-host", default="localhost", show_default=True)
@click.option("-p", "--server-port", default=7687, type=int, show_default=True)
def proxy(bind_host, bind_port, server_host, server_port):
    proxy_server = ProxyServer((bind_host, bind_port), (server_host, server_port))
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
        click.echo(" ".join(e.args))
        exit(1)


@bolt.command(context_settings=dict(
    ignore_unknown_options=True,
), help="""\
Run a Neo4j standalone server or cluster.
""")
@click.option("-c", "--cores", type=int, default=0)
@click.option("-i", "--image", default="latest", show_default=True)
@click.option("-n", "--name")
@click.option("-p", "--password")
@click.option("-r", "--read-replicas", type=int, default=0)
@click.option("-v", "--verbose", is_flag=True)
@click.argument("command", nargs=-1, type=click.UNPROCESSED)
def server(command, cores, image, name, password, read_replicas, verbose):
    if verbose:
        watch("boltkit.containers")
    try:
        with Neo4jService(name or uuid4().hex[-7:],
                          n_cores=cores,
                          n_replicas=read_replicas,
                          image=image,
                          password=password or uuid4().hex) as neo4j:
            addr = " ".join(":".join(map(str, router.bolt_address)) for router in neo4j.routers)
            auth = ":".join(neo4j.machines[0].auth)
            if command:
                run(" ".join(map(shlex_quote, command)), shell=True, env={
                    "NEO4J_ADDR": addr,
                    "NEO4J_AUTH": auth,
                })
            else:
                click.echo("Neo4j available at {}".format(addr))
                click.echo("Press Ctrl+C to exit")
                try:
                    while True:
                        sleep(0.1)
                except KeyboardInterrupt:
                    click.echo("\rShutting down Neo4j")
    except Exception as e:
        click.echo(" ".join(e.args))
        exit(1)


if __name__ == "__main__":
    bolt()
