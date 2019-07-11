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
from shlex import split as shlex_split
from textwrap import wrap
from webbrowser import open as open_browser

from boltkit.addressing import AddressList


log = getLogger("boltkit")


class Neo4jConsole:

    args = None

    def __init__(self, service, read, write):
        self.service = service
        self.read = read
        self.write = write
        self.index = {
            "browser": self.browser,
            "env": self.env,
            "exit": self.exit,
            "help": self.help,
            "logs": self.logs,
            "ls": self.list,
            "ping": self.ping,
            "rt": self.routing_table,
        }

    def run(self):
        self.env()
        while True:
            self.args = shlex_split(self.read(self.service.name))
            command = self.args[0]
            try:
                f = self.index[command]
            except KeyError:
                self.write("Unknown command {!r}".format(command))
            else:
                try:
                    f()
                except RuntimeError as e:
                    log.error(e.args[0])

    def browser(self):
        """ Start the Neo4j browser application for a named machine.
        """
        try:
            name = self.args[1]
        except IndexError:
            name = "a"
        found = 0
        for spec, machine in list(self.service.machines.items()):
            if name in (spec.name, spec.fq_name):
                self.write("Opening web browser for machine {!r} at "
                           "«{}»".format(spec.fq_name, spec.http_uri))
                open_browser(spec.http_uri)
                found += 1
        if not found:
            raise RuntimeError("Machine {} not found".format(name))

    def env(self):
        """ List the environment variables made available by this service.
        """
        for key, value in sorted(self.service.env().items()):
            self.write("%s=%r" % (key, value))

    def exit(self):
        """ Shutdown all machines and exit the console.
        """
        raise SystemExit()

    def help(self):
        """ Show descriptions of all available console commands.
        """
        self.write("Commands:")
        command_width = max(map(len, self.index))
        text_width = 73 - command_width
        template = "  {:<%d}   {}" % command_width
        for command, f in sorted(self.index.items()):
            text = " ".join(line.strip() for line in f.__doc__.splitlines())
            wrapped_text = wrap(text, text_width)
            for i, line in enumerate(wrapped_text):
                if i == 0:
                    self.write(template.format(command, line))
                else:
                    self.write(template.format("", line))

    def list(self):
        """ Show a detailed list of the available servers. Each server is
        listed by name, along with the ports open for Bolt and HTTP traffic,
        the mode in which that server is operating -- CORE, READ_REPLICA or
        SINGLE -- the roles it can fulfil -- (r)ead or (w)rite -- and the
        Docker container in which it runs.
        """
        self.write("NAME        BOLT PORT   HTTP PORT   "
                   "MODE           ROLES   CONTAINER")
        for spec, machine in self.service.machines.items():
            roles = ""
            if machine in self.service.readers:
                roles += "r"
            if machine in self.service.writers:
                roles += "w"
            self.write("{:<12}{:<12}{:<12}{:<15}{:<8}{}".format(
                spec.fq_name,
                spec.bolt_port,
                spec.http_port,
                spec.config.get("dbms.mode", "SINGLE"),
                roles,
                machine.container.short_id,
            ))

    def ping(self):
        """ Ping a server by name to check it is available. If no server name
        is provided, 'a' is used as a default.
        """
        try:
            name = self.args[1]
        except IndexError:
            name = "a"
        found = 0
        for spec, machine in list(self.service.machines.items()):
            if name in (spec.name, spec.fq_name):
                machine.ping(timeout=0)
                found += 1
        if not found:
            raise RuntimeError("Machine {} not found".format(name))

    def routing_table(self):
        """ Fetch an updated routing table and display the contents. The
        routing information is cached so that any subsequent `ls` can show
        role information along with each server.
        """
        self.service.update_routing_info()
        self.write("Routers: «%s»" % AddressList(m.address for m in self.service.routers))
        self.write("Readers: «%s»" % AddressList(m.address for m in self.service.readers))
        self.write("Writers: «%s»" % AddressList(m.address for m in self.service.writers))
        self.write("(TTL: %rs)" % self.service.ttl)

    def logs(self):
        """ Display logs for a named server. If no server name is provided,
        'a' is used as a default.
        """
        try:
            name = self.args[1]
        except IndexError:
            name = "a"
        found = 0
        for spec, machine in list(self.service.machines.items()):
            if name in (spec.name, spec.fq_name):
                self.write(machine.container.logs())
                found += 1
        if not found:
            raise RuntimeError("Machine {} not found".format(name))


class Neo4jClusterConsole(Neo4jConsole):

    def __init__(self, service, read, write):
        super().__init__(service, read, write)
        self.index.update({
            "add-core": self.add_core,
            "add-replica": self.add_replica,
            "rm": self.remove,
        })

    def add_core(self):
        """ Add new core server
        """
        self.service.add_core()

    def add_replica(self):
        """ Add new replica server
        """
        self.service.add_replica()

    def remove(self):
        """ Remove a server by name or role. Servers can be identified either
        by their name (e.g. 'a', 'a.fbe340d') or by the role they fulfil
        (e.g. 'r').
        """
        name = self.args[1]
        found = self.service.remove(name)
        if not found:
            raise RuntimeError("Machine {} not found".format(name))
