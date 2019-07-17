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
from os import makedirs
from os.path import join as path_join
from random import choice
from threading import Thread
from time import monotonic, sleep

from docker import DockerClient
from docker.errors import ImageNotFound

from boltkit.addressing import Address
from boltkit.auth import make_auth
from boltkit.client import AddressList, Connection
from boltkit.server.images import resolve_image
from boltkit.server.console import Neo4jConsole, Neo4jClusterConsole


log = getLogger("boltkit")


class Neo4jDirectorySpec:

    def __init__(self,
                 certificates_dir=None,
                 import_dir=None,
                 logs_dir=None,
                 plugins_dir=None
                 ):
        self.certificates_dir = certificates_dir
        self.import_dir = import_dir
        self.logs_dir = logs_dir
        self.plugins_dir = plugins_dir

    def volumes(self, name):
        volumes = {}
        if self.certificates_dir:
            volumes[self.certificates_dir] = {
                "bind": "/var/lib/neo4j/certificates",
                "mode": "ro",
            }
        if self.import_dir:
            volumes[self.import_dir] = {
                "bind": "/var/lib/neo4j/import",
                "mode": "ro",
            }
        if self.logs_dir:
            volumes[path_join(self.logs_dir, name)] = {
                "bind": "/var/lib/neo4j/logs",
                "mode": "rw",
            }
        if self.plugins_dir:
            volumes[self.plugins_dir] = {
                "bind": "/plugins",
                "mode": "ro",
            }
        return volumes


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

    discovery_port = 5000
    transaction_port = 6000
    raft_port = 7000

    def __init__(self, name, service_name, bolt_port, http_port,
                 dir_spec, config):
        self.name = name
        self.service_name = service_name
        self.bolt_port = bolt_port
        self.http_port = http_port
        self.dir_spec = dir_spec
        self.config = dict(self.config or {})
        self.config["dbms.connector.bolt.advertised_address"] = \
            "localhost:{}".format(self.bolt_port)
        if config:
            self.config.update(**config)

    def __hash__(self):
        return hash(self.fq_name)

    @property
    def fq_name(self):
        return "{}.{}".format(self.name, self.service_name)

    @property
    def discovery_address(self):
        return "{}:{}".format(self.fq_name, self.discovery_port)

    @property
    def transaction_address(self):
        return "{}:{}".format(self.fq_name, self.transaction_port)

    @property
    def raft_address(self):
        return "{}:{}".format(self.fq_name, self.raft_port)

    @property
    def http_uri(self):
        return "http://localhost:{}".format(self.http_port)

    @property
    def bolt_address(self):
        return Address(("localhost", self.bolt_port))


class Neo4jCoreMachineSpec(Neo4jMachineSpec):

    def __init__(self, name, service_name, bolt_port, http_port,
                 dir_spec, config):
        config = config or {}
        config["dbms.mode"] = "CORE"
        super().__init__(name, service_name, bolt_port, http_port,
                         dir_spec, config)


class Neo4jReplicaMachineSpec(Neo4jMachineSpec):

    def __init__(self, name, service_name, bolt_port, http_port,
                 dir_spec, config):
        config = config or {}
        config["dbms.mode"] = "READ_REPLICA"
        super().__init__(name, service_name, bolt_port, http_port,
                         dir_spec, config)


class Neo4jMachine:
    """ A single Neo4j server instance, potentially part of a cluster.
    """

    container = None

    ip_address = None

    ready = 0

    def __init__(self, spec, image, auth):
        self.spec = spec
        self.image = image
        self.address = Address(("localhost", self.spec.bolt_port))
        self.addresses = AddressList([("localhost", self.spec.bolt_port)])
        self.auth = auth
        self.docker = DockerClient.from_env(version="auto")
        environment = {}
        if self.auth:
            environment["NEO4J_AUTH"] = "/".join(self.auth)
        environment["NEO4J_ACCEPT_LICENSE_AGREEMENT"] = "yes"
        for key, value in self.spec.config.items():
            fixed_key = "NEO4J_" + key.replace("_", "__").replace(".", "_")
            environment[fixed_key] = value
        ports = {
            "7474/tcp": self.spec.http_port,
            "7687/tcp": self.spec.bolt_port,
        }
        volumes = self.spec.dir_spec.volumes(self.spec.name)
        for path in volumes:
            makedirs(path, exist_ok=True)

        def create_container(img):
            return self.docker.containers.create(
                img,
                detach=True,
                environment=environment,
                hostname=self.spec.fq_name,
                name=self.spec.fq_name,
                network=self.spec.service_name,
                ports=ports,
                volumes=volumes,
            )

        try:
            self.container = create_container(self.image)
        except ImageNotFound:
            log.info("Downloading Docker image %r", self.image)
            self.docker.images.pull(self.image)
            self.container = create_container(self.image)

    def __hash__(self):
        return hash(self.container)

    def __repr__(self):
        return "%s(fq_name={!r}, image={!r}, address={!r})".format(
            self.__class__.__name__, self.spec.fq_name,
            self.image, self.addresses)

    def start(self):
        log.info("Starting machine %r at "
                 "«%s»", self.spec.fq_name, self.addresses)
        self.container.start()
        self.container.reload()
        self.ip_address = (self.container.attrs["NetworkSettings"]
                           ["Networks"][self.spec.service_name]["IPAddress"])
        log.debug("Machine %r has internal IP address "
                  "«%s»", self.spec.fq_name, self.ip_address)

    def ping(self, timeout):
        try:
            with Connection.open(*self.addresses, auth=self.auth,
                                 timeout=timeout):
                log.info("Machine {!r} available".format(self.spec.fq_name))

        except OSError:
            log.info("Machine {!r} unavailable".format(self.spec.fq_name))

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
                    log.error("Machine %r exited with code %r",
                              self.spec.fq_name, state["ExitCode"])
                    for line in self.container.logs().splitlines():
                        log.error("> %s" % line.decode("utf-8"))
                else:
                    log.error("Machine %r did not become available "
                              "within %rs", self.spec.fq_name, timeout)
            else:
                self.ready = 1
        else:
            log.error("Machine %r is not running (status=%r)",
                      self.spec.fq_name, self.container.status)
            for line in self.container.logs().splitlines():
                log.error("> %s" % line.decode("utf-8"))

    def stop(self):
        log.info("Stopping machine %r", self.spec.fq_name)
        self.container.stop()
        self.container.remove(force=True)


class Neo4jRoutingTable:
    """ Address lists for a Neo4j service.
    """

    def __init__(self, routers=()):
        self.routers = AddressList(routers)
        self.readers = AddressList()
        self.writers = AddressList()
        self.last_updated = 0
        self.ttl = 0

    def update(self, server_lists, ttl):
        new_routers = AddressList()
        new_readers = AddressList()
        new_writers = AddressList()
        for server_list in server_lists:
            role = server_list["role"]
            addresses = map(Address.parse, server_list["addresses"])
            if role == "ROUTE":
                new_routers[:] = addresses
            elif role == "READ":
                new_readers[:] = addresses
            elif role == "WRITE":
                new_writers[:] = addresses
        self.routers[:] = new_routers
        self.readers[:] = new_readers
        self.writers[:] = new_writers
        self.last_updated = monotonic()
        self.ttl = ttl

    def expired(self):
        age = monotonic() - self.last_updated
        return age >= self.ttl

    def age(self):
        age = monotonic() - self.last_updated
        m, s = divmod(age, 60)
        parts = []
        if m:
            parts.append("{:.0f}m".format(m))
        parts.append("{:.0f}s".format(s))
        if age >= self.ttl:
            parts.append("(expired)")
        return " ".join(parts)


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

    def __new__(cls, name=None, image=None, auth=None,
                n_cores=None, n_replicas=None,
                bolt_port=None, http_port=None,
                dir_spec=None, config=None):
        if n_cores:
            return object.__new__(Neo4jClusterService)
        else:
            return object.__new__(Neo4jStandaloneService)

    @classmethod
    def _random_name(cls):
        return "".join(choice("bcdfghjklmnpqrstvwxz") for _ in range(7))

    # noinspection PyUnusedLocal
    def __init__(self, name=None, image=None, auth=None,
                 n_cores=None, n_replicas=None,
                 bolt_port=None, http_port=None,
                 dir_spec=None, config=None):
        self.name = name or self._random_name()
        self.docker = DockerClient.from_env(version="auto")
        self.image = resolve_image(image or self.default_image)
        self.auth = auth or make_auth()
        if self.auth.user != "neo4j":
            raise ValueError("Auth user must be 'neo4j' or empty")
        self.machines = {}
        self.network = None
        self.routing_tables = {"system": Neo4jRoutingTable()}
        self.console = None

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

    def _get_machine_by_address(self, address):
        address = Address((address.host, address.port_number))
        for spec, machine in self.machines.items():
            if spec.bolt_address == address:
                return machine

    def routers(self):
        if self.routing_tables["system"].routers:
            return list(map(self._get_machine_by_address,
                            self.routing_tables["system"].routers))
        else:
            return list(self.machines.values())

    def readers(self, tx_context):
        return list(map(self._get_machine_by_address,
                        self.routing_tables[tx_context].readers))

    def writers(self, tx_context):
        return list(map(self._get_machine_by_address,
                        self.routing_tables[tx_context].writers))

    def ttl(self, context):
        return self.routing_tables[context].ttl

    def _has_valid_routing_table(self, tx_context):
        return (tx_context in self.routing_tables and
                not self.routing_tables[tx_context].expired())

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
            raise RuntimeError("Service %r unavailable - "
                               "some machines failed", self.name)

    def stop(self):
        log.info("Stopping service %r", self.name)
        self._for_each_machine(lambda machine: machine.stop)
        self.network.remove()

    @property
    def addresses(self):
        return AddressList(chain(*(r.addresses for r in self.routers())))

    @classmethod
    def find_and_stop(cls, service_name):
        docker = DockerClient.from_env(version="auto")
        for container in docker.containers.list(all=True):
            if container.name.endswith(".{}".format(service_name)):
                container.stop()
                container.remove(force=True)
        docker.networks.get(service_name).remove()

    def update_routing_info(self, tx_context, force):
        if self._has_valid_routing_table(tx_context) and not force:
            return
        with Connection.open(*self.addresses, auth=self.auth) as cx:
            routing_context = {}
            records = []
            if cx.bolt_version >= 4:
                run = cx.run("CALL dbms.cluster.routing."
                             "getRoutingTable($rc, $tc)", {
                                 "rc": routing_context,
                                 "tc": tx_context,
                             })
            else:
                run = cx.run("CALL dbms.cluster.routing."
                             "getRoutingTable($rc)", {
                                 "rc": routing_context,
                             })
            cx.pull(-1, -1, records)
            cx.send_all()
            cx.fetch_all()
            if run.error:
                raise run.error
            if records:
                ttl, server_lists = records[0]
                rt = self.routing_tables.setdefault(tx_context,
                                                    Neo4jRoutingTable())
                rt.update(server_lists, ttl)
            else:
                raise RuntimeError("No routing data available for "
                                   "context {!r}".format(tx_context))

    def run_console(self):
        self.console = Neo4jConsole(self)
        self.console.invoke("env")
        self.console.run()

    def env(self):
        addr = AddressList(chain(*(r.addresses for r in self.routers())))
        auth = "{}:{}".format(self.auth.user, self.auth.password)
        return {
            "BOLT_SERVER_ADDR": str(addr),
            "NEO4J_AUTH": auth,
        }


class Neo4jStandaloneService(Neo4jService):

    default_image = "neo4j:latest"

    def __init__(self, name=None, image=None, auth=None,
                 n_cores=None, n_replicas=None,
                 bolt_port=None, http_port=None,
                 dir_spec=None, config=None):
        super().__init__(name, image, auth,
                         n_cores, n_replicas,
                         bolt_port, http_port,
                         dir_spec, config)
        spec = Neo4jMachineSpec(
            name="a",
            service_name=self.name,
            bolt_port=bolt_port or self.default_bolt_port,
            http_port=http_port or self.default_http_port,
            dir_spec=dir_spec,
            config=config,
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
    max_replicas = 9

    default_bolt_port = 17601
    default_http_port = 17401

    @classmethod
    def _port_range(cls, base_port, count):
        return range(base_port, base_port + count)

    def __init__(self, name=None, image=None, auth=None,
                 n_cores=None, n_replicas=None,
                 bolt_port=None, http_port=None,
                 dir_spec=None, config=None):
        super().__init__(name, image, auth,
                         n_cores, n_replicas,
                         bolt_port, http_port,
                         dir_spec, config)
        n_cores = n_cores or self.min_cores
        n_replicas = n_replicas or self.min_replicas
        if not self.min_cores <= n_cores <= self.max_cores:
            raise ValueError("A cluster must have been {} and {} "
                             "cores".format(self.min_cores, self.max_cores))
        if not self.min_replicas <= n_replicas <= self.max_replicas:
            raise ValueError("A cluster must have been {} and {} "
                             "read replicas".format(self.min_replicas,
                                                    self.max_replicas))

        core_bolt_port_range = self._port_range(
            bolt_port or self.default_bolt_port, self.max_cores)
        core_http_port_range = self._port_range(
            http_port or self.default_http_port, self.max_cores)
        self.free_core_machine_specs = [
            Neo4jCoreMachineSpec(
                name=chr(97 + i),
                service_name=self.name,
                bolt_port=core_bolt_port_range[i],
                http_port=core_http_port_range[i],
                dir_spec=dir_spec,
                config=dict(config or {}, **{
                    "causal_clustering.minimum_core_cluster_size_at_formation":
                        n_cores or self.min_cores,
                    "causal_clustering.minimum_core_cluster_size_at_runtime":
                        self.min_cores,
                }),
            )
            for i in range(self.max_cores)
        ]
        replica_bolt_port_range = self._port_range(
            ceil(core_bolt_port_range.stop / 10) * 10 + 1, self.max_replicas)
        replica_http_port_range = self._port_range(
            ceil(core_http_port_range.stop / 10) * 10 + 1, self.max_replicas)
        self.free_replica_machine_specs = [
            Neo4jReplicaMachineSpec(
                name=chr(49 + i),
                service_name=self.name,
                bolt_port=replica_bolt_port_range[i],
                http_port=replica_http_port_range[i],
                dir_spec=dir_spec,
                config=config,
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
                    "causal_clustering.initial_discovery_members":
                        ",".join(discovery_addresses),
                })
                self.machines[spec] = Neo4jMachine(spec, self.image, self.auth)

    def cores(self):
        return [machine for spec, machine in self.machines.items()
                if isinstance(spec, Neo4jCoreMachineSpec)]

    def replicas(self):
        return [machine for spec, machine in self.machines.items()
                if isinstance(spec, Neo4jReplicaMachineSpec)]

    def routers(self):
        if self.routing_tables["system"].routers:
            return list(map(self._get_machine_by_address,
                            self.routing_tables["system"].routers))
        else:
            return list(self.cores())

    def run_console(self):
        self.console = Neo4jClusterConsole(self)
        self.console.run()

    def add_core(self):
        """ Add new core server
        """
        if len(self.cores()) < self.max_cores:
            spec = self.free_core_machine_specs.pop(0)
            self.machines[spec] = None
            self._boot_machines()
            self.machines[spec].start()
            self.machines[spec].await_started(300)
        else:
            raise RuntimeError("A maximum of {} cores "
                               "is permitted".format(self.max_cores))

    def add_replica(self):
        """ Add new replica server
        """
        if len(self.replicas()) < self.max_replicas:
            spec = self.free_replica_machine_specs.pop(0)
            self.machines[spec] = None
            self._boot_machines()
            self.machines[spec].start()
            self.machines[spec].await_started(300)
        else:
            raise RuntimeError("A maximum of {} replicas "
                               "is permitted".format(self.max_replicas))

    def _remove_machine(self, spec):
        machine = self.machines[spec]
        del self.machines[spec]
        machine.stop()
        if isinstance(spec, Neo4jCoreMachineSpec):
            self.free_core_machine_specs.append(spec)
        elif isinstance(spec, Neo4jReplicaMachineSpec):
            self.free_replica_machine_specs.append(spec)

    def remove(self, name):
        """ Remove a server by name or role. Servers can be identified either
        by their name (e.g. 'a', 'a.fbe340d') or by the role they fulfil
        (e.g. 'r').
        """
        found = 0
        for spec, machine in list(self.machines.items()):
            if (name == "r" and self.readers is not None and
                    machine in self.readers):
                self._remove_machine(spec)
                found += 1
            elif (name == "w" and self.writers is not None and
                  machine in self.writers):
                self._remove_machine(spec)
                found += 1
            elif name in (spec.name, spec.fq_name):
                self._remove_machine(spec)
                found += 1
        return found
