import asyncio
import json
import logging
import signal
import sys

from pax.protocol import (
    ConnectionServerProtocol,
    PeerClientProtocol,
)
from pax.primitive import (
    SuggestionId,
    Proposer,
    Learner,
)


log = logging.getLogger(__name__)

SERIAL = 0

class PaxServer:

    def __init__(self, name, conf):
        self.name = name
        self.address = conf[name]
        self.peers = conf
        self.peer_protocols = {}
        self.last_accepted_value = None
        self.last_accepted_id = None
        self.learner = Learner(None)

    async def connect_peers(self):
        loop = asyncio.get_running_loop()
        tasks = []
        for name in self.peers:
            task = await self.connect_peer(name)
            if task:
                tasks.append(task)
        while tasks:
            _tasks = tasks
            tasks = []
            for _task in list(_tasks):
                _tasks.remove(_task)
                task = await _task
                if task:
                    tasks.append(task)

    async def _connect_peer(self, name):
        loop = asyncio.get_running_loop()
        on_con_lost = loop.create_future()
        addr = self.peers[name]
        transport, protocol = await loop.create_connection(
            lambda: PeerClientProtocol(name, on_con_lost),
            *addr)
        await protocol.connection_event.wait()
        message = "Hello {} from {}".format(name, self.name)
        data = json.dumps({"from": "peer", "message": message}).encode()
        transport.write(data)
        return protocol

    async def connect_peer(self, name, wait=0):
        loop = asyncio.get_running_loop()
        await asyncio.sleep(wait)
        try:
            protocol = await self._connect_peer(name)
        except ConnectionRefusedError:
            log.error("Unable to connect to %s", name)
            task = loop.create_task(self.connect_peer(name, wait=5))
            return task
        self.peer_protocols[name] = protocol

    async def serve_forever(self):
        loop = asyncio.get_running_loop()
        protocol = ConnectionServerProtocol(self)
        server = await loop.create_server(
            lambda: protocol,
            *self.address)
        def handle_sig_term(*args, **kwargs):
            log.warning("Closing server connections")
            server.close()
        loop.add_signal_handler(signal.SIGTERM, handle_sig_term)
        loop.add_signal_handler(signal.SIGINT, handle_sig_term)
        await self.connect_peers()
        log.warning("Peers %r", self.peer_protocols)
        async with server:
            try:
                await server.serve_forever()
            except asyncio.exceptions.CancelledError:
                pass

    async def propose(self, value):
        loop = asyncio.get_running_loop()
        global SERIAL
        sug_id = SuggestionId(SERIAL, self.name)
        SERIAL += 1
        proposer = Proposer(
            self.peers,
            sug_id,
            value,
        )
        tasks = []
        for peer in self.peers:
            task = loop.create_task(self.peer_protocols[peer].permission(str(sug_id)))
            tasks.append(task)
        for name, result in await asyncio.gather(*tasks):
            if result:
                proposer.grant(name)
        tasks = []
        if not proposer.can_propose:
            raise Exception("Unable to propose value")
        for peer in self.peers:
            task = loop.create_task(self.peer_protocols[peer].propose(str(sug_id), value))
            tasks.append(task)
        for name, result in await asyncio.gather(*tasks):
            if result:
                proposer.accept(name)
        if not proposer.value_accepted:
            raise Exception("Value not accepted")
        tasks = []
        for peer in self.peers:
            task = loop.create_task(self.peer_protocols[peer].learn(value))
            tasks.append(task)
        for name, result in await asyncio.gather(*tasks):
            log.info("Learner response %s %r", name, result)
        return str(sug_id), value



from pax import parse_conf


async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    conf = parse_conf()
    name = sys.argv[-1]

    pax = PaxServer(name, conf)
    await pax.serve_forever()


asyncio.run(main())
