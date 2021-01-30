import asyncio
import json
import logging
import uuid

from .primitive import SuggestionId, Acceptor, Learner


log = logging.getLogger(__name__)


class PeerClientProtocol(asyncio.Protocol):

    def __init__(self, name, on_con_lost):
        self.name = name
        self.on_con_lost = on_con_lost
        self.transport = None
        self.connection_event = asyncio.Event()
        self.permission_reqs = {}
        self.proposal_reqs = {}
        self.learn_reqs = {}

    def connection_made(self, transport):
        self.transport = transport
        self.connection_event.set()
        #self.transport.write(self.message.encode())
        #log.info("Data sent: %r", self.message)

    def data_received(self, data):
        log.info("Data received: %r", data.decode())
        msg = json.loads(data.decode())
        if 'action' in msg:
            if msg['action'] == 'grant':
                fut = self.permission_reqs.pop(msg['sug_id'])
                fut.set_result((self.name, True))
            elif msg['action'] == 'accept':
                fut = self.proposal_reqs.pop(msg['sug_id'])
                fut.set_result((self.name, True))
            elif msg['action'] == 'learned':
                fut = self.learn_reqs.pop(msg['learned_id'])
                fut.set_result((self.name, True))

    def connection_lost(self, exc):
        log.info("The server closed the connection")
        self.connection_event.clear()
        self.on_con_lost.set_result(True)

    async def permission(self, sug_id):
        loop = asyncio.get_running_loop()
        data = json.dumps({
            'from': 'peer',
            'action': 'permission',
            'sug_id': sug_id,
        }).encode()
        future = loop.create_future()
        self.permission_reqs[sug_id] = future
        self.transport.write(data)
        return await future

    async def propose(self, sug_id, value):
        loop = asyncio.get_running_loop()
        data = json.dumps({
            'from': 'peer',
            'action': 'proposal',
            'sug_id': sug_id,
            'value': value,
        }).encode()
        future = loop.create_future()
        self.proposal_reqs[sug_id] = future
        self.transport.write(data)
        return await future

    async def learn(self, value):
        loop = asyncio.get_running_loop()
        learned_id = str(uuid.uuid4())
        data = json.dumps({
            'from': 'peer',
            'action': 'learn',
            'value': value,
            'learned_id': learned_id,
        }).encode()
        future = loop.create_future()
        self.learn_reqs[learned_id] = future
        self.transport.write(data)
        return await future


class PeerProtocol(asyncio.Protocol):

    def __init__(self, server, peername=None, transport=None):
        self.server = server
        self.peername = peername
        self.transport = transport
        self.grant = None
        self.acceptor = Acceptor(SuggestionId(0, '0'), None)
        super().__init__()

    def connection_made(self, transport):
        raise Exception("Not new connection")

    def data_received(self, data):
        message = json.loads(data.decode())
        log.info("Data received: %r %r", self, message)
        if 'action' in message:
            if message['action'] == 'permission':
                log.warning("handle permission request %r", message)
                sug_id = SuggestionId.parse(message['sug_id'])
                if self.acceptor.should_grant(sug_id):
                    message = {
                        'from': 'peer',
                        'action': 'grant',
                        'sug_id': message['sug_id']
                    }
                    self.acceptor.grant(sug_id)
                    self.transport.write(json.dumps(message).encode())
                else:
                    log.warning("DOING NOTHING - should send NACK %r", message)
            elif message['action'] == 'proposal':
                log.warning("handle proposal request %r", message)
                sug_id = SuggestionId.parse(message['sug_id'])
                if self.acceptor.should_accept(sug_id):
                    value = message['value']
                    self.acceptor.accept(sug_id, value)
                    self.server.last_accepted_id = self.acceptor.last_id
                    self.server.last_accepted_value = self.acceptor.value
                    message = {
                        'from': 'peer',
                        'action': 'accept',
                        'sug_id': message['sug_id'],
                        'value': message['value']
                    }
                    self.transport.write(json.dumps(message).encode())
            elif message['action'] == 'learn':
                log.warning("handle learn request %r", message)
                value = message['value']
                learned_id = message['learned_id']
                self.server.learner = Learner(value)
                message = {
                    'from': 'peer',
                    'action': 'learned',
                    'value': value,
                    'learned_id': learned_id,
                }
                self.transport.write(json.dumps(message).encode())
        else:
            log.info("Send: %r %s", self.peername, message)
            self.transport.write(data)


class ClientProtocol(asyncio.Protocol):

    def __init__(self, server, peername=None, transport=None):
        self.server = server
        self.peername = peername
        self.transport = transport
        super().__init__()

    def connection_made(self, transport):
        raise Exception("Not new connection")

    def data_received(self, data):
        loop = asyncio.get_running_loop()
        message = json.loads(data.decode())
        if 'action' in message:
            if message['action'] == 'propose':
                value = message['value']
                task = loop.create_task(self.server.propose(value))
                task.add_done_callback(self.send_propose_reply)
            elif message['action'] == 'query':
                data = json.dumps({
                    'last_accepted_id': str(self.server.last_accepted_id),
                    'last_accepted_value': self.server.last_accepted_value,
                    'learned_value': self.server.learner.value
                }).encode()
                self.transport.write(data)
        else:
            # fall back to echo
            log.info("Data received: %r %r", self, message)
            log.info("Send: %r %s", self.peername, message)
            self.transport.write(data)

    def send_propose_reply(self, task):
        sug_id, value = task.result()
        data = json.dumps({
            'action': 'accepted',
            'sug_id': sug_id,
            'value': value,
        }).encode()
        self.transport.write(data)


class DispatchProtocol(asyncio.Protocol):

    def __init__(self, server, peername=None, transport=None):
        self.server = server
        self.peername = peername
        self.transport = transport
        super().__init__()

    def connection_made(self, transport):
        raise Exception("Not new connection")

    def data_received(self, data):
        message = json.loads(data.decode())
        if message['from'] == 'peer':
            proto_cls = PeerProtocol
        elif message['from'] == 'client':
            proto_cls = ClientProtocol
        transport = self.transport
        protocol = proto_cls(self.server, self.peername, transport)
        self.transport = None
        transport.set_protocol(protocol)
        return protocol.data_received(data)


class ConnectionServerProtocol(asyncio.Protocol):

    def __init__(self, server, peername=None, transport=None):
        self.server = server
        self.peername = peername
        self.transport = transport
        super().__init__()

    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        proto = DispatchProtocol(self.server, peername, transport)
        transport.set_protocol(proto)
        log.info("Connection from %s", peername)

    def data_received(self, data):
        raise Exception("Not able to handle data")
