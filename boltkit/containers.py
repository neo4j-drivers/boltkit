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


from logging import getLogger
from threading import Thread
from time import sleep, time

from docker import DockerClient

from boltkit.client import Connection


log = getLogger("boltkit.containers")


class Neo4jMachine:
    """ A single Neo4j server instance, potentially part of a cluster.
    """

    repository = "neo4j"

    server_start_timeout = 120

    def __init__(self, name, network, image, bolt_address, http_address, auth, **config):
        self.name = name
        self.network = network
        self.image = "{}:{}".format(self.repository, image)
        self.bolt_address = bolt_address
        self.http_address = http_address
        self.auth = auth
        self.docker = DockerClient.from_env()
        environment = {
            "NEO4J_AUTH": "/".join(self.auth),
            "NEO4J_ACCEPT_LICENSE_AGREEMENT": "yes",
        }
        for key, value in config.items():
            environment["NEO4J_" + key.replace("_", "__").replace(".", "_")] = value
        ports = {
            "7474/tcp": self.http_address,
            "7687/tcp": self.bolt_address,
        }
        self.container = self.docker.containers.create(self.image,
                                                       detach=True,
                                                       environment=environment,
                                                       hostname="{}.{}".format(self.name, self.network.name),
                                                       name="{}.{}".format(self.name, self.network.name),
                                                       network=self.network.name,
                                                       ports=ports)

        self.ip_address = None

    def __del__(self):
        try:
            self.container.remove()
        except AttributeError:
            pass

    def __hash__(self):
        return hash(self.container)

    def __repr__(self):
        return "%s(...)" % self.__class__.__name__

    def start(self):
        log.debug("Starting Docker container %r with image %r", self.container.name, self.image)
        self.container.start()
        self.container.reload()
        self.ip_address = self.container.attrs["NetworkSettings"]["Networks"][self.network.name]["IPAddress"]

    def await_ready(self):
        log.debug("Waiting for server %r on address %r to become available", self.container.name, self.ip_address)
        t0 = time()
        while time() < t0 + self.server_start_timeout:
            try:
                Connection.open(self.bolt_address, auth=self.auth).close()
            except OSError:
                sleep(1)
            else:
                log.debug("Server %r is now available", self.container.name)
                return
        raise RuntimeError("Server %r did not become available in %r seconds" % (self.container.name, self.server_start_timeout))

    def stop(self):
        log.debug("Stopping Docker container %r", self.container.name)
        self.container.stop()


class Neo4jService:
    """ A Neo4j database management service.
    """

    @classmethod
    def fix_image(cls, image):
        """ Apply a default if no image is supplied.
        """
        return image or "latest"

    def __new__(cls, name, **parameters):
        n_cores = parameters.get("n_cores")
        if n_cores:
            return object.__new__(Neo4jClusterService)
        else:
            return object.__new__(Neo4jStandaloneService)

    def __init__(self, name, **parameters):
        self.docker = DockerClient.from_env()
        self.image = self.fix_image(parameters.get("image"))
        self.user = parameters.get("user", "neo4j")
        self.password = parameters.get("password", "password")
        self.network = self.docker.networks.create(name)
        self.machines = []
        self.routers = []
        self.bolt_port_range = range(17600, 17700)
        self.http_port_range = range(17400, 17500)

    def __del__(self):
        try:
            self.network.remove()
        except AttributeError:
            pass

    def __enter__(self):
        self.start()
        self.await_ready()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def start(self):
        threads = []
        for machine in self.machines:
            thread = Thread(target=machine.start)
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()

    def await_ready(self):
        threads = []
        for machine in self.machines:
            thread = Thread(target=machine.await_ready)
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()

    def stop(self):
        threads = []
        for machine in self.machines:
            thread = Thread(target=machine.stop)
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()


class Neo4jStandaloneService(Neo4jService):

    def __init__(self, name, **parameters):
        super().__init__(name, **parameters)
        self.machines.append(Neo4jMachine(
            "z",
            self.network,
            self.image,
            bolt_address=("localhost", self.bolt_port_range[0]),
            http_address=("localhost", self.http_port_range[0]),
            auth=(self.user, self.password),
        ))
        self.routers.extend(self.machines)


class Neo4jClusterService(Neo4jService):

    # The minimum and maximum number of cores permitted
    min_cores = 3
    max_cores = 9

    # The minimum and maximum number of read replicas permitted
    min_replicas = 0
    max_replicas = 10

    @classmethod
    def fix_image(cls, image):
        """ Force the image name to be an Enterprise distribution.
        """
        image = super().fix_image(image)
        if image == "latest":
            return "enterprise"
        elif image.endswith("enterprise"):
            return image
        else:
            return "{}-enterprise".format(image)

    def __init__(self, name, n_cores=3, n_replicas=0, **parameters):
        if not self.min_cores <= n_cores <= self.max_cores:
            raise ValueError("A cluster must have been {} and {} cores".format(self.min_cores, self.max_cores))
        if not self.min_replicas <= n_replicas <= self.max_replicas:
            raise ValueError("A cluster must have been {} and {} read replicas".format(self.min_replicas, self.max_replicas))
        super().__init__(name, n_cores=n_cores, n_replicas=n_replicas, **parameters)
        core_names = [chr(i) for i in range(97, 97 + n_cores)]
        replica_names = [chr(i) for i in range(48, 48 + n_replicas)]
        q_core_names = ["{}.{}".format(name, self.network.name) for name in core_names]
        core_members = ["{}:5000".format(q_name) for q_name in q_core_names]
        self.machines.extend(Neo4jMachine(
            core_names[i],
            self.network,
            self.image,
            bolt_address=("localhost", self.bolt_port_range[i + 1]),
            http_address=("localhost", self.http_port_range[i + 1]),
            auth=(self.user, self.password),
            **{
                "causal_clustering.initial_discovery_members": ",".join(core_members),
                "causal_clustering.minimum_core_cluster_size_at_formation": n_cores,
                "causal_clustering.minimum_core_cluster_size_at_runtime": self.min_cores,
                "dbms.connector.bolt.advertised_address": "localhost:{}".format(self.bolt_port_range[i + 1]),
                "dbms.mode": "CORE",
            }
        ) for i in range(n_cores or 0))
        self.routers.extend(self.machines)
        self.machines.extend(Neo4jMachine(
            replica_names[i],
            self.network,
            self.image,
            bolt_address=("localhost", self.bolt_port_range[i + 10]),
            http_address=("localhost", self.http_port_range[i + 10]),
            auth=(self.user, self.password),
            **{
                "causal_clustering.initial_discovery_members": ",".join(core_members),
                "dbms.connector.bolt.advertised_address": "localhost:{}".format(self.bolt_port_range[i + 10]),
                "dbms.mode": "READ_REPLICA",
            }
        ) for i in range(n_replicas or 0))
