#!/usr/bin/env python

"""Tests for `tornado_bunny` package."""


import asyncio
import logging
import sys
import os
from typing import Any

import pytest
from aio_pika import RobustConnection, Message, IncomingMessage

from tornado_bunny import AsyncConnection, ChannelConfiguration, RabbitMQConnectionData


def run_coroutine_to_completion(loop: asyncio.AbstractEventLoop, coroutine, *args, **kwargs) -> Any:
    return loop.run_until_complete(asyncio.gather(coroutine(*args, **kwargs)))[0]



