import asyncio
import json
import logging
import signal
import sys
import os

from pax.protocol import (
    ConnectionServerProtocol,
    PeerClientProtocol,
)
from pax.primitive import (
    SuggestionId,
    Proposer,
    Learner,
)
from pax.wal import Table


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
        self.datadir = os.path.join('var', self.name)
        try:
            os.makedirs(self.datadir)
        except FileExistsError:
            pass

    async def init_table(self):
        table_path = os.path.join(self.datadir, 'root')
        self.table = Table('root', table_path)
        await self.table.open()

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

    async def connect_peer(self, name, num_tries=0, wait=0):
        num_tries += 1
        loop = asyncio.get_running_loop()
        await asyncio.sleep(wait)
        try:
            protocol = await self._connect_peer(name)
        except ConnectionRefusedError:
            if num_tries > 2:
                log.error("Unable to connect to %s", name)
            task = loop.create_task(self.connect_peer(name, wait=.3))
            return task
        self.peer_protocols[name] = protocol
        if len(self.peer_protocols) == len(self.peers):
            log.info("Node %s is connected to all of it's peers (%d)",
                self.name, len(self.peers),
            )

    async def serve_forever(self):
        loop = asyncio.get_running_loop()
        await self.init_table()
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
        log.debug("Peers %r", self.peer_protocols)
        async with server:
            try:
                await server.serve_forever()
            except asyncio.exceptions.CancelledError:
                pass

    async def propose(self, key, value):
        loop = asyncio.get_running_loop()
        serial = self.table.wid
        sug_id = SuggestionId(serial, self.name)
        proposer = Proposer(
            self.peers,
            sug_id,
            [key, value],
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
            task = loop.create_task(self.peer_protocols[peer].propose(str(sug_id), [key, value]))
            tasks.append(task)
        for name, result in await asyncio.gather(*tasks):
            if result:
                proposer.accept(name)
        if not proposer.value_accepted:
            raise Exception("Value not accepted")
        tasks = []
        for peer in self.peers:
            task = loop.create_task(self.peer_protocols[peer].learn([key, value]))
            tasks.append(task)
        for name, result in await asyncio.gather(*tasks):
            log.debug("Learner response %s %r", name, result)
        return str(sug_id), value



from pax import parse_conf


async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    conf = parse_conf()
    name = sys.argv[-1]

    pax = PaxServer(name, conf)
    await pax.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
