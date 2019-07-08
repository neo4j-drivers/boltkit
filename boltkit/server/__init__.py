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
from logging import getLogger
from math import ceil
from os import getenv
from shlex import split as shlex_split
from threading import Thread
from time import sleep
from uuid import uuid4
from xml.etree import ElementTree

import certifi
from docker import DockerClient
from docker.errors import ImageNotFound
from urllib3 import PoolManager, make_headers

from boltkit.addressing import Address
from boltkit.auth import make_auth
from boltkit.client import AddressList, Connection


TEAMCITY_USER = getenv("TEAMCITY_USER")
TEAMCITY_PASSWORD = getenv("TEAMCITY_PASSWORD")


log = getLogger("boltkit")


class Neo4jMachineSpec:

    # Base config for all machines. This can be overridden by
    # individual instances.
    config = {
        "dbms.backup.enabled": "false",
        "dbms.memory.heap.initial_size": "300m",
        "dbms.memory.heap.max_size": "500m",
        "dbms.memory.pagecache.size": "50m",
        "dbms.transaction.bookmark_ready_timeout": "5s",
    }

    def __init__(self, name, service_name, bolt_port, http_port, **config):
        self.name = name
        self.service_name = service_name
        self.bolt_port = bolt_port
        self.http_port = http_port
        self.config = dict(self.config)
        self.config["dbms.connector.bolt.advertised_address"] = "localhost:{}".format(self.bolt_port)
        self.config.update(**config)

    def __hash__(self):
        return hash(self.fq_name)

    @property
    def fq_name(self):
        return "{}.{}".format(self.name, self.service_name)

    @property
    def discovery_address(self):
        return "{}.{}:5000".format(self.name, self.service_name)

    @property
    def http_uri(self):
        return "http://localhost:{}".format(self.http_port)


class Neo4jCoreMachineSpec(Neo4jMachineSpec):

    def __init__(self, name, service_name, bolt_port, http_port, **config):
        config["dbms.mode"] = "CORE"
        super().__init__(name, service_name, bolt_port, http_port, **config)


class Neo4jReplicaMachineSpec(Neo4jMachineSpec):

    def __init__(self, name, service_name, bolt_port, http_port, **config):
        config["dbms.mode"] = "READ_REPLICA"
        super().__init__(name, service_name, bolt_port, http_port, **config)


class Neo4jMachine:
    """ A single Neo4j server instance, potentially part of a cluster.
    """

    container = None

    ip_address = None

    ready = 0

    def __init__(self, spec, image, auth):
        self.spec = spec
        self.image = image
        self.addresses = AddressList([("localhost", self.spec.bolt_port)])
        self.auth = auth
        self.docker = DockerClient.from_env(version="auto")
        environment = {}
        if self.auth:
            environment["NEO4J_AUTH"] = "{}/{}".format(self.auth[0], self.auth[1])
        if "enterprise" in image:
            environment["NEO4J_ACCEPT_LICENSE_AGREEMENT"] = "yes"
        for key, value in self.spec.config.items():
            environment["NEO4J_" + key.replace("_", "__").replace(".", "_")] = value
        ports = {
            "7474/tcp": self.spec.http_port,
            "7687/tcp": self.spec.bolt_port,
        }

        def create_container(img):
            return self.docker.containers.create(img,
                                                 detach=True,
                                                 environment=environment,
                                                 hostname=self.spec.fq_name,
                                                 name=self.spec.fq_name,
                                                 network=self.spec.service_name,
                                                 ports=ports)

        try:
            self.container = create_container(self.image)
        except ImageNotFound:
            log.info("Downloading Docker image %r", self.image)
            self.docker.images.pull(self.image)
            self.container = create_container(self.image)

    def __hash__(self):
        return hash(self.container)

    def __repr__(self):
        return "%s(fq_name=%r, image=%r, address=%r)" % (self.__class__.__name__, self.spec.fq_name, self.image, self.addresses)

    def start(self):
        log.info("Starting machine %r at «%s»", self.spec.fq_name, self.addresses)
        self.container.start()
        self.container.reload()
        self.ip_address = self.container.attrs["NetworkSettings"]["Networks"][self.spec.service_name]["IPAddress"]

    def ping(self, timeout):
        Connection.open(*self.addresses, auth=self.auth, timeout=timeout).close()

    def await_started(self, timeout):
        sleep(1)
        self.container.reload()
        if self.container.status == "running":
            try:
                self.ping(timeout)
            except OSError:
                self.container.reload()
                state = self.container.attrs["State"]
                if state["Status"] == "exited":
                    self.ready = -1
                    log.error("Machine %r exited with code %r", self.spec.fq_name, state["ExitCode"])
                    for line in self.container.logs().splitlines():
                        log.error("> %s" % line.decode("utf-8"))
                else:
                    log.error("Machine %r did not become available within %rs", self.spec.fq_name, timeout)
            else:
                self.ready = 1
        else:
            log.error("Machine %r is not running (status=%r)", self.spec.fq_name, self.container.status)
            for line in self.container.logs().splitlines():
                log.error("> %s" % line.decode("utf-8"))

    def stop(self):
        log.info("Stopping machine %r", self.spec.fq_name)
        self.container.stop()
        self.container.remove(force=True)


class Neo4jService:
    """ A Neo4j database management service.
    """

    default_image = NotImplemented

    default_bolt_port = 7687
    default_http_port = 7474

    snapshot_host = "live.neo4j-build.io"
    snapshot_build_config_id = "Neo4j40_Docker"
    snapshot_build_url = ("https://{}/repository/download/{}/"
                          "lastSuccessful".format(snapshot_host,
                                                  snapshot_build_config_id))

    console_read = None
    console_write = None
    console_args = None
    console_index = None

    def __new__(cls, name=None, n_cores=None, **parameters):
        if n_cores:
            return object.__new__(Neo4jClusterService)
        else:
            return object.__new__(Neo4jStandaloneService)

    def __init__(self, name=None, image=None, auth=None, **parameters):
        self.name = name or uuid4().hex[-7:]
        self.docker = DockerClient.from_env(version="auto")
        headers = {}
        if TEAMCITY_USER and TEAMCITY_PASSWORD:
            headers.update(make_headers(
                basic_auth="{}:{}".format(TEAMCITY_USER, TEAMCITY_PASSWORD)))
        self.http = PoolManager(
            cert_reqs="CERT_REQUIRED",
            ca_certs=certifi.where(),
            headers=headers,
        )
        self.image = self._resolve_image(image)
        self.auth = auth or make_auth()
        if self.auth.user != "neo4j":
            raise ValueError("Auth user must be 'neo4j' or empty")
        self.machines = {}
        self.network = None
        self.console_index = {}

    def __enter__(self):
        try:
            self.start(timeout=300)
        except KeyboardInterrupt:
            self.stop()
            raise
        else:
            return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    @property
    def routers(self):
        return list(self.machines.values())

    def _resolve_image(self, image):
        resolved = image or self.default_image
        if ":" not in resolved:
            resolved = "neo4j:" + image
        if resolved == "neo4j:snapshot":
            return self._pull_snapshot("community")
        elif resolved in ("neo4j:snapshot-enterprise",
                          "neo4j-enterprise:snapshot"):
            return self._pull_snapshot("enterprise")
        else:
            return resolved

    def _resolve_artifact_name(self, edition):
        log.info("Resolving snapshot artifact name on «{}»".format(
            self.snapshot_host))
        prefix = "neo4j-{}".format(edition)
        url = "{}/teamcity-ivy.xml".format(self.snapshot_build_url)
        r1 = self.http.request("GET", url)
        root = ElementTree.fromstring(r1.data)
        for e in root.find("publications").findall("artifact"):
            attr = e.attrib
            if attr["type"] == "tar" and attr["name"].startswith(prefix):
                return "{}.{}".format(attr["name"], attr["ext"])

    @classmethod
    def _derive_image_tag(cls, artifact_name):
        if artifact_name.endswith("-docker-complete.tar"):
            artifact_name = artifact_name[:-20]
        else:
            raise ValueError("Expected artifact name to end with "
                             "'-docker-complete.tar'")
        if artifact_name.startswith("neo4j-enterprise-"):
            return "neo4j-enterprise:{}".format(artifact_name[17:])
        elif artifact_name.startswith("neo4j-community-"):
            return "neo4j:{}".format(artifact_name[16:])
        else:
            raise ValueError("Expected artifact name to start with either "
                             "'neo4j-community-' or 'neo4j-enterprise-'")

    def _pull_snapshot(self, edition):
        artifact = self._resolve_artifact_name(edition)
        derived = self._derive_image_tag(artifact)
        try:
            self.docker.images.get(derived)
        except ImageNotFound:
            log.info("Downloading {} from «{}»".format(
                artifact, self.snapshot_host))
            url = "{}/{}".format(self.snapshot_build_url, artifact)
            r2 = self.http.request("GET", url)
            images = self.docker.images.load(r2.data)
            image = images[0]
            return image.tags[0]
        else:
            return derived

    def _for_each_machine(self, f):
        threads = []
        for spec, machine in self.machines.items():
            thread = Thread(target=f(machine))
            thread.daemon = True
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()

    def start(self, timeout=None):
        log.info("Starting service %r with image %r", self.name, self.image)
        self.network = self.docker.networks.create(self.name)
        self._for_each_machine(lambda machine: machine.start)
        if timeout is not None:
            self.await_started(timeout)

    def await_started(self, timeout):

        def wait(machine):
            machine.await_started(timeout=timeout)

        self._for_each_machine(wait)
        if all(machine.ready == 1 for spec, machine in self.machines.items()):
            log.info("Service %r available", self.name)
        else:
            log.error("Service %r unavailable - some machines failed", self.name)
            raise OSError("Some machines failed")

    def stop(self):
        log.info("Stopping service %r", self.name)
        self._for_each_machine(lambda machine: machine.stop)
        self.network.remove()

    @property
    def addresses(self):
        return AddressList(chain(*(r.addresses for r in self.routers)))

    @classmethod
    def find_and_stop(cls, service_name):
        docker = DockerClient.from_env(version="auto")
        for container in docker.containers.list(all=True):
            if container.name.endswith(".{}".format(service_name)):
                container.stop()
                container.remove(force=True)
        docker.networks.get(service_name).remove()

    def get_routing_info(self):
        with Connection.open(*self.addresses, auth=self.auth) as cx:
            records = []
            cx.run("CALL dbms.cluster.routing.getRoutingTable($context)",
                   {"context": {}})
            cx.pull(-1, -1, records)
            cx.send_all()
            cx.fetch_all()
            ttl, server_lists = records[0]
            routers = AddressList()
            readers = AddressList()
            writers = AddressList()
            for server_list in server_lists:
                role = server_list["role"]
                addresses = map(Address.parse, server_list["addresses"])
                if role == "ROUTE":
                    routers[:] = addresses
                elif role == "READ":
                    readers[:] = addresses
                elif role == "WRITE":
                    writers[:] = addresses
            return ttl, {
                "routers": routers,
                "readers": readers,
                "writers": writers,
            }

    def _update_console_index(self):
        self.console_index.update({
            "env": self.console_env,
            "exit": self.console_exit,
            "help": self.console_help,
            "logs": self.console_logs,
            "ls": self.console_list,
            "ping": self.console_ping,
            "routing": self.console_routing,
        })

    def console(self, read, write):
        self.console_read = read
        self.console_write = write
        self._update_console_index()
        self.console_env()
        while True:
            self.console_args = shlex_split(self.console_read(self.name))
            try:
                f = self.console_index[self.console_args[0]]
            except KeyError:
                self.console_write("ERROR!")
            else:
                f()

    def env(self):
        """ Show environment variables
        """
        addr = AddressList(chain(*(r.addresses for r in self.routers)))
        auth = "{}:{}".format(self.auth.user, self.auth.password)
        return {
            "BOLT_SERVER_ADDR": str(addr),
            "NEO4J_AUTH": auth,
        }

    def console_env(self):
        """ Show environment variables
        """
        for key, value in sorted(self.env().items()):
            self.console_write("%s=%r" % (key, value))

    def console_exit(self):
        """ Shutdown and exit
        """
        raise SystemExit()

    def console_help(self):
        """ Display help
        """
        width = max(map(len, self.console_index))
        template = "{:<%d}   {}" % width
        for command, f in sorted(self.console_index.items()):
            self.console_write(template.format(command, f.__doc__.strip()))

    def console_list(self):
        """ List active servers
        """
        ttl, servers = self.get_routing_info()
        writers = servers["writers"]
        self.console_write("NAME        BOLT PORT   HTTP PORT   "
                           "MODE           CONTAINER")
        for spec, machine in self.machines.items():
            self.console_write("{:<12}{:<12}{:<11}{}{:<15}{}".format(
                spec.fq_name,
                spec.bolt_port,
                spec.http_port,
                "*" if ("localhost", str(spec.bolt_port)) in writers else " ",
                spec.config.get("dbms.mode", "SINGLE"),
                machine.container.short_id,
            ))

    def console_ping(self):
        """ Ping server
        """
        try:
            name = self.console_args[1]
        except IndexError:
            name = "a"
        found = 0
        for spec, machine in list(self.machines.items()):
            if name in (spec.name, spec.fq_name):
                machine.ping(timeout=0)
                found += 1
        if not found:
            self.console_write("Machine {} not found".format(name))

    def console_routing(self):
        """ Show routing table
        """
        ttl, servers = self.get_routing_info()
        self.console_write("Routers: «%s»" % servers["routers"])
        self.console_write("Readers: «%s»" % servers["readers"])
        self.console_write("Writers: «%s»" % servers["writers"])
        self.console_write("(TTL: %rs)" % ttl)

    def console_logs(self):
        """ Display server logs
        """
        try:
            name = self.console_args[1]
        except IndexError:
            name = "a"
        found = 0
        for spec, machine in list(self.machines.items()):
            if name in (spec.name, spec.fq_name):
                self.console_write(machine.container.logs())
                found += 1
        if not found:
            self.console_write("Machine {} not found".format(name))


class Neo4jStandaloneService(Neo4jService):

    default_image = "neo4j:latest"

    def __init__(self, name=None, bolt_port=None, http_port=None, **parameters):
        super().__init__(name, **parameters)
        spec = Neo4jMachineSpec(
            "a",
            self.name,
            bolt_port=bolt_port or self.default_bolt_port,
            http_port=http_port or self.default_http_port,
        )
        self.machines[spec] = Neo4jMachine(
            spec,
            self.image,
            auth=self.auth,
        )


class Neo4jClusterService(Neo4jService):

    default_image = "neo4j:enterprise"

    # The minimum and maximum number of cores permitted
    min_cores = 3
    max_cores = 7

    # The minimum and maximum number of read replicas permitted
    min_replicas = 0
    max_replicas = 10

    default_bolt_port = 17601
    default_http_port = 17401

    @classmethod
    def _port_range(cls, base_port, count):
        return range(base_port, base_port + count)

    def __init__(self, name=None, bolt_port=None, http_port=None, n_cores=None, n_replicas=None, **parameters):
        super().__init__(name, n_cores=n_cores, n_replicas=n_replicas, **parameters)
        n_cores = n_cores or self.min_cores
        n_replicas = n_replicas or self.min_replicas
        if not self.min_cores <= n_cores <= self.max_cores:
            raise ValueError("A cluster must have been {} and {} cores".format(self.min_cores, self.max_cores))
        if not self.min_replicas <= n_replicas <= self.max_replicas:
            raise ValueError("A cluster must have been {} and {} read replicas".format(self.min_replicas, self.max_replicas))

        core_bolt_port_range = self._port_range(bolt_port or self.default_bolt_port, self.max_cores)
        core_http_port_range = self._port_range(http_port or self.default_http_port, self.max_cores)
        self.free_core_machine_specs = [
            Neo4jCoreMachineSpec(
                chr(97 + i),
                self.name,
                core_bolt_port_range[i],
                core_http_port_range[i],
                **{
                    "causal_clustering.minimum_core_cluster_size_at_formation": n_cores or self.min_cores,
                    "causal_clustering.minimum_core_cluster_size_at_runtime": self.min_cores,
                }
            )
            for i in range(self.max_cores)
        ]
        replica_bolt_port_range = self._port_range(ceil(core_bolt_port_range.stop / 10) * 10, self.max_replicas)
        replica_http_port_range = self._port_range(ceil(core_http_port_range.stop / 10) * 10, self.max_replicas)
        self.free_replica_machine_specs = [
            Neo4jReplicaMachineSpec(
                chr(48 + i),
                self.name,
                replica_bolt_port_range[i],
                replica_http_port_range[i],
            )
            for i in range(self.max_replicas)
        ]

        # Add core machine specs
        for i in range(n_cores or self.min_cores):
            spec = self.free_core_machine_specs.pop(0)
            self.machines[spec] = None
        # Add replica machine specs
        for i in range(n_replicas or self.min_replicas):
            spec = self.free_replica_machine_specs.pop(0)
            self.machines[spec] = None

        self._boot_machines()

    def _boot_machines(self):
        discovery_addresses = [spec.discovery_address for spec in self.machines
                               if isinstance(spec, Neo4jCoreMachineSpec)]
        for spec, machine in self.machines.items():
            if machine is None:
                spec.config.update({
                    "causal_clustering.initial_discovery_members": ",".join(discovery_addresses),
                })
                self.machines[spec] = Neo4jMachine(spec, self.image, self.auth)

    @property
    def cores(self):
        return [machine for spec, machine in self.machines.items()
                if isinstance(spec, Neo4jCoreMachineSpec)]

    @property
    def replicas(self):
        return [machine for spec, machine in self.machines.items()
                if isinstance(spec, Neo4jReplicaMachineSpec)]

    @property
    def routers(self):
        return list(self.cores)

    def _update_console_index(self):
        super()._update_console_index()
        self.console_index.update({
            "add-core": self.console_add_core,
            "add-replica": self.console_add_replica,
            "rm": self.console_remove,
        })

    def console_add_core(self):
        """ Add new core server
        """
        if len(self.cores) < self.max_cores:
            spec = self.free_core_machine_specs.pop(0)
            self.machines[spec] = None
            self._boot_machines()
            self.machines[spec].start()
            self.machines[spec].await_started(300)
            self.console_write("Added core server %r" % spec.fq_name)
        else:
            self.console_write("A maximum of {} cores "
                               "is permitted".format(self.max_cores))

    def console_add_replica(self):
        """ Add new replica server
        """
        if len(self.replicas) < self.max_replicas:
            spec = self.free_replica_machine_specs.pop(0)
            self.machines[spec] = None
            self._boot_machines()
            self.machines[spec].start()
            self.machines[spec].await_started(300)
            self.console_write("Added replica server %r" % spec.fq_name)
        else:
            self.console_write("A maximum of {} replicas "
                               "is permitted".format(self.max_replicas))

    def console_remove(self):
        """ Remove server by name
        """
        name = self.console_args[1]
        found = 0
        for spec, machine in list(self.machines.items()):
            if name in (spec.name, spec.fq_name):
                del self.machines[spec]
                machine.stop()
                if isinstance(spec, Neo4jCoreMachineSpec):
                    self.free_core_machine_specs.append(spec)
                elif isinstance(spec, Neo4jReplicaMachineSpec):
                    self.free_replica_machine_specs.append(spec)
                found += 1
        if not found:
            self.console_write("Machine {} not found".format(name))
