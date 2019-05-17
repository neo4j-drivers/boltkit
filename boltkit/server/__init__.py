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
from threading import Thread
from uuid import uuid4

from docker import DockerClient
from docker.errors import APIError, ImageNotFound

from boltkit.auth import make_auth
from boltkit.client import AddressList, Connection


log = getLogger("boltkit")


class Neo4jMachine:
    """ A single Neo4j server instance, potentially part of a cluster.
    """

    ip_address = None

    ready = 0

    def __init__(self, name, service_name, image, auth, bolt_port, http_port, **config):
        self.name = name
        self.service_name = service_name
        self.fq_name = "{}.{}".format(self.name, self.service_name)
        self.image = image
        self.bolt_port = bolt_port
        self.http_port = http_port
        self.address = AddressList([("localhost", self.bolt_port)])
        self.auth = auth
        self.docker = DockerClient.from_env(version="auto")
        environment = {}
        if self.auth:
            environment["NEO4J_AUTH"] = "{}/{}".format(self.auth.user, self.auth.password)
        if "enterprise" in image:
            environment["NEO4J_ACCEPT_LICENSE_AGREEMENT"] = "yes"
        for key, value in config.items():
            environment["NEO4J_" + key.replace("_", "__").replace(".", "_")] = value
        ports = {
            "7474/tcp": self.http_port,
            "7687/tcp": self.bolt_port,
        }

        def create_container(img):
            return self.docker.containers.create(img,
                                                 detach=True,
                                                 environment=environment,
                                                 hostname=self.fq_name,
                                                 name=self.fq_name,
                                                 network=self.service_name,
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
        return "%s(fq_name=%r, image=%r, address=%r)" % (self.__class__.__name__, self.fq_name, self.image, self.address)

    def __del__(self):
        if self.container:
            try:
                self.container.remove(force=True)
            except APIError:
                pass

    def start(self):
        log.info("Starting machine %r at «%s»", self.fq_name, self.address)
        self.container.start()
        self.container.reload()
        self.ip_address = self.container.attrs["NetworkSettings"]["Networks"][self.service_name]["IPAddress"]

    def await_started(self, timeout):
        try:
            Connection.open(*self.address, auth=self.auth, timeout=timeout).close()
        except OSError:
            self.container.reload()
            state = self.container.attrs["State"]
            if state["Status"] == "exited":
                self.ready = -1
                log.error("Machine %r exited with code %r" % (self.fq_name, state["ExitCode"]))
                for line in self.container.logs().splitlines():
                    log.error("> %s" % line.decode("utf-8"))
            else:
                log.error("Machine %r did not become available within %rs" % (self.fq_name, timeout))
        else:
            self.ready = 1
            # log.info("Machine %r available", self.name)

    def stop(self):
        log.info("Stopping machine %r", self.fq_name)
        self.container.stop()


class Neo4jService:
    """ A Neo4j database management service.
    """

    default_image = NotImplemented

    default_bolt_port = 7687
    default_http_port = 7474

    def __new__(cls, name=None, n_cores=None, **parameters):
        if n_cores:
            return object.__new__(Neo4jClusterService)
        else:
            return object.__new__(Neo4jStandaloneService)

    def __init__(self, name=None, image=None, auth=None, **parameters):
        self.name = name or uuid4().hex[-7:]
        self.docker = DockerClient.from_env(version="auto")
        self.image = image or self.default_image
        if ":" not in self.image:
            self.image = "neo4j:" + image
        self.auth = auth or make_auth()
        self.machines = []
        self.routers = []
        self.network = None

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

    def __del__(self):
        if self.network:
            try:
                self.network.remove()
            except APIError:
                pass

    def _for_each_machine(self, f):
        threads = []
        for machine in self.machines:
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
        if all(machine.ready == 1 for machine in self.machines):
            log.info("Service %r available", self.name)
        else:
            log.error("Service %r unavailable - some machines failed", self.name)
            raise OSError("Some machines failed")

    def stop(self):
        log.info("Stopping service %r", self.name)
        self._for_each_machine(lambda machine: machine.stop)

    @property
    def address(self):
        return AddressList(chain(*(r.address for r in self.routers)))


class Neo4jStandaloneService(Neo4jService):

    default_image = "neo4j:latest"

    def __init__(self, name=None, bolt_port=None, http_port=None, **parameters):
        super().__init__(name, **parameters)
        self.machines.append(Neo4jMachine(
            "z",
            self.name,
            self.image,
            auth=self.auth,
            bolt_port=bolt_port or self.default_bolt_port,
            http_port=http_port or self.default_http_port,
        ))
        self.routers.extend(self.machines)


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
        self.n_cores = n_cores or self.min_cores
        self.n_replicas = n_replicas or self.min_replicas
        if not self.min_cores <= self.n_cores <= self.max_cores:
            raise ValueError("A cluster must have been {} and {} cores".format(self.min_cores, self.max_cores))
        if not self.min_replicas <= self.n_replicas <= self.max_replicas:
            raise ValueError("A cluster must have been {} and {} read replicas".format(self.min_replicas, self.max_replicas))

        # CORES
        # =====
        # Calculate port numbers for Bolt
        core_bolt_port_range = self._port_range(bolt_port or self.default_bolt_port, self.max_cores)
        # Calculate port numbers for HTTP
        core_http_port_range = self._port_range(http_port or self.default_http_port, self.max_cores)
        # Calculate machine names
        core_names = [chr(i) for i in range(97, 97 + self.n_cores)]
        core_addresses = ["{}.{}:5000".format(name, self.name) for name in core_names]
        #
        self.machines.extend(Neo4jMachine(
            core_names[i],
            self.name,
            self.image,
            auth=self.auth,
            bolt_port=core_bolt_port_range[i],
            http_port=core_http_port_range[i],
            **{
                "causal_clustering.initial_discovery_members": ",".join(core_addresses),
                "causal_clustering.minimum_core_cluster_size_at_formation": self.n_cores,
                "causal_clustering.minimum_core_cluster_size_at_runtime": self.min_cores,
                "dbms.connector.bolt.advertised_address": "localhost:{}".format(core_bolt_port_range[i]),
                "dbms.mode": "CORE",
            }
        ) for i in range(self.n_cores or 0))
        self.routers.extend(self.machines)

        # REPLICAS
        # ========
        # Calculate port numbers for Bolt
        replica_bolt_port_range = self._port_range(ceil(core_bolt_port_range.stop / 10) * 10, self.max_replicas)
        # Calculate port numbers for HTTP
        replica_http_port_range = self._port_range(ceil(core_http_port_range.stop / 10) * 10, self.max_replicas)
        # Calculate machine names
        replica_names = [chr(i) for i in range(48, 48 + self.n_replicas)]
        #
        self.machines.extend(Neo4jMachine(
            replica_names[i],
            self.name,
            self.image,
            auth=self.auth,
            bolt_port=replica_bolt_port_range[i],
            http_port=replica_http_port_range[i],
            **{
                "causal_clustering.initial_discovery_members": ",".join(core_addresses),
                "dbms.connector.bolt.advertised_address": "localhost:{}".format(replica_bolt_port_range[i]),
                "dbms.mode": "READ_REPLICA",
            }
        ) for i in range(self.n_replicas or 0))
