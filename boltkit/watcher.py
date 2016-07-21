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


from logging import CRITICAL, DEBUG, ERROR, INFO, WARNING, Formatter, StreamHandler, getLogger
from sys import stdout


def black(s):
    return "\x1b[30m{:s}\x1b[0m".format(s)


def red(s):
    return "\x1b[31m{:s}\x1b[0m".format(s)


def green(s):
    return "\x1b[32m{:s}\x1b[0m".format(s)


def yellow(s):
    return "\x1b[33m{:s}\x1b[0m".format(s)


def blue(s):
    return "\x1b[34m{:s}\x1b[0m".format(s)


def magenta(s):
    return "\x1b[35m{:s}\x1b[0m".format(s)


def cyan(s):
    return "\x1b[36m{:s}\x1b[0m".format(s)


def white(s):
    return "\x1b[36m{:s}\x1b[0m".format(s)


def bright_black(s):
    return "\x1b[30;1m{:s}\x1b[0m".format(s)


def bright_red(s):
    return "\x1b[31;1m{:s}\x1b[0m".format(s)


def bright_green(s):
    return "\x1b[32;1m{:s}\x1b[0m".format(s)


def bright_yellow(s):
    return "\x1b[33;1m{:s}\x1b[0m".format(s)


def bright_blue(s):
    return "\x1b[34;1m{:s}\x1b[0m".format(s)


def bright_magenta(s):
    return "\x1b[35;1m{:s}\x1b[0m".format(s)


def bright_cyan(s):
    return "\x1b[36;1m{:s}\x1b[0m".format(s)


def bright_white(s):
    return "\x1b[37;1m{:s}\x1b[0m".format(s)


class ColourFormatter(Formatter):

    def format(self, record):
        s = super(ColourFormatter, self).format(record)
        if record.levelno == CRITICAL:
            return bright_red(s)
        elif record.levelno == ERROR:
            return bright_yellow(s)
        elif record.levelno == WARNING:
            return yellow(s)
        elif record.levelno == INFO:
            return cyan(s)
        elif record.levelno == DEBUG:
            return blue(s)
        else:
            return s


class Watcher(object):
    """ Log watcher for monitoring driver and protocol activity.
    """

    handlers = {}

    def __init__(self, logger_name):
        super(Watcher, self).__init__()
        self.logger_name = logger_name
        self.logger = getLogger(self.logger_name)
        self.formatter = ColourFormatter("%(asctime)s  %(message)s")

    def watch(self, level=INFO, out=stdout):
        self.stop()
        handler = StreamHandler(out)
        handler.setFormatter(self.formatter)
        self.handlers[self.logger_name] = handler
        self.logger.addHandler(handler)
        self.logger.setLevel(level)

    def stop(self):
        try:
            self.logger.removeHandler(self.handlers[self.logger_name])
        except KeyError:
            pass


def watch(logger_name, level=INFO, out=stdout):
    """ Quick wrapper for using the Watcher.

    :param logger_name: name of logger to watch
    :param level: minimum log level to show (default INFO)
    :param out: where to send output (default stdout)
    :return: Watcher instance
    """
    watcher = Watcher(logger_name)
    watcher.watch(level, out)
    return watcher
