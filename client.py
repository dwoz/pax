import asyncio
import json
import logging
import sys
import signal
from pax import parse_conf


log = logging.getLogger(__name__)


class ServerClientProtocol(asyncio.Protocol):

    def __init__(self, on_con_lost, on_con_made):
        self.on_con_lost = on_con_lost
        self.on_con_made = on_con_made
        self.reply = None

    def connection_made(self, transport):
        self.transport = transport
        self.on_con_made.set_result(True)

    def data_received(self, data):
        self.reply.set_result(data)

    def connection_lost(self, exc):
        log.info("The server closed the connection")
        self.on_con_lost.set_result(True)
        if self.reply:
            if exc is None:
                exc = Exception("Connection closed")
            self.reply.set_exception(exc)

    async def send(self, message):
        loop = asyncio.get_running_loop()
        self.reply = loop.create_future()
        data = json.dumps({'from': 'client', 'message': message}).encode()
        log.debug("Data sent: %r", data)
        self.transport.write(data)
        data = await self.reply
        log.debug("Data received: %r", data.decode())
        return data

    async def propose(self, key, value):
        loop = asyncio.get_running_loop()
        self.reply = loop.create_future()
        data = json.dumps({
            'from': 'client',
            'action': 'propose',
            'key': key,
            'value': value,
        }).encode()
        log.debug("Data sent: %r", data)
        self.transport.write(data)
        data = await self.reply
        log.debug("Data received: %r", data.decode())
        return data

    async def query(self, key):
        loop = asyncio.get_running_loop()
        self.reply = loop.create_future()
        data = json.dumps({
            'key': key,
            'from': 'client',
            'action': 'query',
        }).encode()
        log.debug("Data sent: %r", data)
        self.transport.write(data)
        data = await self.reply
        log.debug("Data received: %r", data.decode())
        return data


class PaxClient:
    def __init__(self, name, conf):
        self.name = name
        self.addr = conf[name]
        self.conf = conf
        self.transport = None
        loop = asyncio.get_running_loop()
        def handle_sig_term(*args, **kwargs):
            if self.transport:
                self.transport.close()
        loop.add_signal_handler(signal.SIGTERM, handle_sig_term)
        loop.add_signal_handler(signal.SIGINT, handle_sig_term)


    async def __call__(self, message):
        loop = asyncio.get_running_loop()
        on_con_lost = loop.create_future()
        on_con_made = loop.create_future()
        self.transport, protocol = await loop.create_connection(
            lambda: ServerClientProtocol(on_con_lost, on_con_made),
            *self.addr)
        await on_con_made
        data = await protocol.send(message)

    async def propose(self, key, value):
        loop = asyncio.get_running_loop()
        on_con_lost = loop.create_future()
        on_con_made = loop.create_future()
        self.transport, protocol = await loop.create_connection(
            lambda: ServerClientProtocol(on_con_lost, on_con_made),
            *self.addr)
        await on_con_made
        data = await protocol.propose(key, value)
        return data

    async def query(self, key):
        loop = asyncio.get_running_loop()
        on_con_lost = loop.create_future()
        on_con_made = loop.create_future()
        self.transport, protocol = await loop.create_connection(
            lambda: ServerClientProtocol(on_con_lost, on_con_made),
            *self.addr)
        await on_con_made
        data = await protocol.query(key)
        return json.loads(data)['value']

import argparse


parser = argparse.ArgumentParser("Pax Client")
parser.add_argument('peer_name')
parser.add_argument('key') #, nargs='?', default=None)
parser.add_argument('value', nargs='?', default=None)


async def main():
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s [client] %(levelname)s %(message)s")
    ns = parser.parse_args()
    conf = parse_conf()
    name = ns.peer_name
    key = ns.key
    value = ns.value
    log.debug("Name: %s Key: %s Value: %s", name, key, value)
    client = PaxClient(name, conf)
    if value is None:
        value = await client.query(key)
        print(value)
    else:
        await client.propose(key, value)


asyncio.run(main())
