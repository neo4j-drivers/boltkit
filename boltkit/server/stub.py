#!/usr/bin/env python
# coding: utf-8

# Copyright (c) 2002-2019 "Neo Technology,"
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


from asyncio import new_event_loop, start_server, sleep, CancelledError, ensure_future, \
    set_event_loop
from logging import getLogger
from threading import Thread

from boltkit.addressing import Address
from boltkit.packstream import PackStream
from boltkit.server.scripting import ServerExit, ClientMessageMismatch, BoltScript, \
    ClientMessageLine


log = getLogger("boltkit")


class BoltStubService:

    default_base_port = 17601

    default_timeout = 30

    thread = None

    auth = ("neo4j", "")

    @classmethod
    def load(cls, *script_filenames, **kwargs):
        return cls(*map(BoltScript.load, script_filenames), **kwargs)

    def __init__(self, *scripts, listen_addr=None, exit_on_disconnect=True, timeout=None):
        if listen_addr:
            listen_addr = Address(listen_addr)
        else:
            listen_addr = Address(("localhost", self.default_base_port))
        self.exit_on_disconnect = exit_on_disconnect
        self.timeout = timeout or self.default_timeout
        self.loop = new_event_loop()
        self.loop.set_debug(True)
        self.sleeper = None
        self.host = listen_addr.host
        self.next_free_port = listen_addr.port_number
        self.scripts = {}
        for script in scripts:
            if script.port:
                address = Address((listen_addr.host, script.port))
            else:
                address = Address((listen_addr.host, self.next_free_port))
                self.next_free_port += 1
            self.scripts[address.port_number] = script
        self.servers = {}
        self.exit_code = 0

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        self.wait_stopped()

    @property
    def addresses(self):
        return sorted(Address((self.host, port)) for port in self.scripts)

    def start(self):
        if self.thread and self.thread.is_alive():
            raise RuntimeError("Already running")
        try:
            self._start_servers()
        except KeyboardInterrupt:
            self.exit_code = 130
            self._stop_servers()
        else:
            self.thread = Thread(target=self._serve, daemon=True)
            self.thread.start()

    def stop(self):
        if self.sleeper:
            self.loop.call_soon_threadsafe(self.sleeper.cancel)
            self.sleeper = None

    def wait_stopped(self):
        if self.thread and self.thread.is_alive():
            self.thread.join()

    def _start_servers(self):
        self.servers.clear()
        for port_number, script in self.scripts.items():
            address = Address((self.host, port_number))
            server = self.loop.run_until_complete(
                start_server(self._handshake, host=self.host, port=port_number))
            log.debug("[#%04X]  S: <LISTEN> %s (%s)", port_number, address, script.filename)
            self.servers[port_number] = server

    def _stop_servers(self):
        for server in self.servers.values():
            server.close()
            self.loop.run_until_complete(server.wait_closed())
        self.loop.stop()
        self.loop.close()

    def _serve(self):
        set_event_loop(self.loop)
        try:
            self.sleeper = ensure_future(sleep(self.timeout, loop=self.loop), loop=self.loop)
            self.loop.run_until_complete(self.sleeper)
        except CancelledError:
            pass
        except KeyboardInterrupt:
            self.exit_code = 130
        else:
            print("Timed out after {!r}s".format(self.timeout))
            self.exit_code = 99
        finally:
            self._stop_servers()

    async def _handshake(self, reader, writer):
        client_address = Address(writer.transport.get_extra_info("peername"))
        server_address = Address(writer.transport.get_extra_info("sockname"))
        script = self.scripts[server_address.port_number]
        log.debug("[#%04X]  S: <ACCEPT> %s -> %s", server_address.port_number,
                  client_address, server_address)
        try:
            request = await reader.readexactly(20)
            log.debug("[#%04X]  C: <HANDSHAKE> %r", server_address.port_number, request)
            response = script.on_handshake(request)
            log.debug("[#%04X]  S: <HANDSHAKE> %r", server_address.port_number, response)
            writer.write(response)
            await writer.drain()
            actor = BoltActor(script, reader, writer)
            await actor.play()
        except ServerExit:
            pass
        except ClientMessageMismatch as error:
            print(error)
            if error.line_no:
                print("(in {!r} at line {})".format(error.script.filename, error.line_no))
            else:
                print("(in {!r})".format(error.script.filename))
            self.exit_code = 1
        except Exception as error:
            print("{}: {}".format(error.__class__.__name__, error))
            self.exit_code = 2
        finally:
            try:
                log.debug("[#%04X]  S: <HANGUP>", server_address.port_number)
                writer.write_eof()
                writer.close()
            except OSError:
                pass
            await self._on_disconnect(server_address.port_number)

    async def _on_disconnect(self, port):
        if self.exit_on_disconnect:
            server = self.servers[port]
            server.close()
            await server.wait_closed()
            del self.servers[port]
            if not self.servers:
                self.stop()


class BoltActor:

    def __init__(self, script, reader, writer):
        self.script = script
        self.reader = reader
        self.writer = writer
        self.stream = PackStream(reader, writer)

    @property
    def server_address(self):
        return Address(self.writer.transport.get_extra_info("sockname"))

    async def play(self):
        for line in self.script:
            try:
                await line.action(self)
            except ClientMessageMismatch as error:
                # Attach context information and re-raise
                error.script = self.script
                error.line_no = line.line_no
                raise
        await ClientMessageLine.default_action(self)

    def log(self, text, *args):
        log.debug("[#%04X]  " + text, self.server_address.port_number, *args)

    def log_error(self, text, *args):
        log.error("[#%04X]  " + text, self.server_address.port_number, *args)


def stub_test(*scripts):
    """ Decorator for stub tests.
    """
    def f__(f):
        def f_(*args, **kwargs):
            s = map(BoltScript.load, scripts)
            service = BoltStubService(*s, timeout=5)
            service.start()
            kwargs["server"] = service
            yield f(*args, **kwargs)
            service.wait_stopped()
            if service.exit_code != 0:
                assert False, "Stub test failed with exit code {}".format(service.exit_code)
        f_.__name__ = f.__name__
        f_.__doc__ = f.__doc__
        f_.__dict__.update(f.__dict__)
        return f_
    return f__
