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
from time import sleep, time

from docker import DockerClient

from boltkit.client import Connection


default_bolt_interface = "localhost"
default_bolt_port = 17687
default_http_interface = "localhost"
default_http_port = 17474
default_user = "neo4j"
default_password = "password"


log = getLogger("boltkit.containers")


class Neo4jContainer:

    server_start_timeout = 120

    def __init__(self, image, bolt_address=None, http_address=None, auth=None):
        self.bolt_address = bolt_address or (default_bolt_interface, default_bolt_port)
        self.http_address = http_address or (default_http_interface, default_http_port)
        self.auth = auth or (default_user, default_password)
        self.docker = DockerClient.from_env()
        if ":" not in image:
            image = "neo4j:{}".format(image)
        self.image = image
        environment = {"NEO4J_AUTH": "/".join(self.auth)}
        ports = {
            "7474/tcp": self.http_address,
            "7687/tcp": self.bolt_address,
        }
        self.container = self.docker.containers.create(image, detach=True, environment=environment, ports=ports)

    def start(self):
        log.debug("Starting Docker container %r with image %r", self.container.name, self.image)
        self.container.start()

    def await_bolt(self):
        log.debug("Waiting for server to become available")
        t0 = time()
        while time() < t0 + self.server_start_timeout:
            try:
                Connection.open(self.bolt_address, auth=self.auth).close()
            except OSError:
                sleep(1)
            else:
                log.debug("Server is now available")
                return
        raise RuntimeError("Server did not become available in %r seconds" % self.server_start_timeout)

    def __repr__(self):
        return "%s(...)" % self.__class__.__name__

    def __enter__(self):
        self.start()
        self.await_bolt()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def stop(self):
        log.debug("Stopping Docker container %r", self.container.name)
        self.container.stop()
