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

from docker import DockerClient

from boltkit.auth import make_auth
from boltkit.client import Addr, Connection


log = getLogger("boltkit")


class Neo4jMachine:
    """ A single Neo4j server instance, potentially part of a cluster.
    """

    repository = "neo4j"

    ip_address = None

    ready = 0

    def __init__(self, name, service_name, image, bolt_address, http_address, auth, **config):
        self.name = name
        self.service_name = service_name
        self.image = "{}:{}".format(self.repository, image)
        self.address = Addr([bolt_address])
        self.auth = auth or make_auth()
        self.docker = DockerClient.from_env()
        environment = {}
        if self.auth:
            environment["NEO4J_AUTH"] = "{}/{}".format(self.auth.user, self.auth.password)
        if "enterprise" in image:
            environment["NEO4J_ACCEPT_LICENSE_AGREEMENT"] = "yes"
        for key, value in config.items():
            environment["NEO4J_" + key.replace("_", "__").replace(".", "_")] = value
        ports = {
            "7474/tcp": http_address,
            "7687/tcp": bolt_address,
        }
        self.name = "{}.{}".format(self.name, self.service_name)
        self.container = self.docker.containers.create(self.image,
                                                       detach=True,
                                                       environment=environment,
                                                       hostname=self.name,
                                                       name=self.name,
                                                       network=self.service_name,
                                                       ports=ports)

    def __hash__(self):
        return hash(self.container)

    def __repr__(self):
        return "%s(...)" % self.__class__.__name__

    def start(self):
        log.info("Starting machine %r at %r", self.name, self.address)
        self.container.start()
        self.container.reload()
        self.ip_address = self.container.attrs["NetworkSettings"]["Networks"][self.service_name]["IPAddress"]

    def await_ready(self, timeout):
        try:
            Connection.open(*self.address, auth=self.auth, timeout=timeout).close()
        except OSError:
            self.container.reload()
            state = self.container.attrs["State"]
            if state["Status"] == "exited":
                self.ready = -1
                log.error("Machine %r exited with code %r" % (self.name, state["ExitCode"]))
                for line in self.container.logs().splitlines():
                    log.error("> %s" % line.decode("utf-8"))
            else:
                log.error("Machine %r did not become available within %rs" % (self.name, timeout))
        else:
            self.ready = 1
            # log.info("Machine %r available", self.name)

    def stop(self):
        log.info("Stopping machine %r", self.name)
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
        self.name = name
        self.docker = DockerClient.from_env()
        self.image = self.fix_image(parameters.get("image"))
        self.auth = parameters.get("auth")
        self.machines = []
        self.routers = []
        self.bolt_port_range = range(17600, 17700)
        self.http_port_range = range(17400, 17500)
        self.network = None

    def __enter__(self):
        try:
            self.start()
            self.await_ready()
        except KeyboardInterrupt:
            self.stop()
            raise
        else:
            return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def _for_each_machine(self, f):
        threads = []
        for machine in self.machines:
            thread = Thread(target=f(machine))
            thread.daemon = True
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()

    def start(self):
        log.info("Starting service %r with image %r", self.name, self.image)
        self.network = self.docker.networks.create(self.name)
        self._for_each_machine(lambda machine: machine.start)

    def await_ready(self):

        def wait(machine):
            machine.await_ready(timeout=300)

        self._for_each_machine(wait)
        if all(machine.ready == 1 for machine in self.machines):
            log.info("Service %r available", self.name)
        else:
            log.error("Service %r unavailable - some machines failed", self.name)
            raise OSError("Some machines failed")

    def stop(self):
        log.info("Stopping service %r", self.name)
        self._for_each_machine(lambda machine: machine.stop)
        self.network.remove()


class Neo4jStandaloneService(Neo4jService):

    def __init__(self, name, **parameters):
        super().__init__(name, **parameters)
        self.machines.append(Neo4jMachine(
            "x",
            name,
            self.image,
            bolt_address=("localhost", self.bolt_port_range[0]),
            http_address=("localhost", self.http_port_range[0]),
            auth=self.auth
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
        q_core_names = ["{}.{}".format(name, self.name) for name in core_names]
        core_members = ["{}:5000".format(q_name) for q_name in q_core_names]
        self.machines.extend(Neo4jMachine(
            core_names[i],
            name,
            self.image,
            bolt_address=("localhost", self.bolt_port_range[i + 1]),
            http_address=("localhost", self.http_port_range[i + 1]),
            auth=self.auth,
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
            name,
            self.image,
            bolt_address=("localhost", self.bolt_port_range[i + 10]),
            http_address=("localhost", self.http_port_range[i + 10]),
            auth=self.auth,
            **{
                "causal_clustering.initial_discovery_members": ",".join(core_members),
                "dbms.connector.bolt.advertised_address": "localhost:{}".format(self.bolt_port_range[i + 10]),
                "dbms.mode": "READ_REPLICA",
            }
        ) for i in range(n_replicas or 0))
