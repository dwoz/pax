import io
import socket
import threading
import queue
import functools
import time
import sys
import json
import logging
import struct


log = logging.getLogger(__name__)


SERIAL = 0


def exception_logger(func):
    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            log.exception("Unhandled exception")
    return wrapped



class Message(object):
    _kind = ''
    _classes = {}

    @property
    def kind(self):
        return self._kind

    @classmethod
    def register_kind(cls, sub_cls):
        cls._classes[sub_cls._kind] = (sub_cls.loads, sub_cls.dumps)

    @classmethod
    def dumps(cls, obj, encoding='utf-8'):
        return cls._classes[obj._kind][1](obj).encode(encoding)

    @classmethod
    def loads(cls, val, encoding='utf-8'):
        data = json.loads(val.decode('utf-8'))
        return cls._classes[data['kind']][0](data)


class SuggestionId(object):


    def __init__(self, uid, node):
        self.uid = uid
        self.node = node

    def __str__(self):
        return "{},{}".format(self.uid, self.node)

    def __repr__(self):
        return "<SuggestionId({}, {}) {}>".format(
            self.uid, self.node, id(self)
        )

    @classmethod
    def parse(cls, val):
        _, node = val.split(',')
        uid = int(_)
        return cls(uid, node)

    def __gt__(self, other):
        return self.uid >= other.uid and self.node > other.node

    def __lt__(self, other):
        return self.uid <= other.uid and self.node < other.node

    def __eq__(self, other):
        return self.uid == other.uid and self.node == other.node


class PermReq(Message):
    _kind = 'permission_request'

    def __init__(self, suggestion_id):
        self.suggestion_id = suggestion_id

    @classmethod
    def dumps(cls, obj):
        return json.dumps({
            'kind': obj.kind,
            'id': str(obj.suggestion_id),
        })

    @classmethod
    def loads(cls, data):
        return cls(SuggestionId.parse(data['id']))


Message.register_kind(PermReq)



class PermGrant(Message):
    _kind = 'permission_granted'

    def  __init__(self, suggestion_id, last_id, last_value):
        self.suggestion_id = suggestion_id
        self.last_id = last_id
        self.last_value = last_value

    @classmethod
    def dumps(cls, obj):
        return json.dumps({
            'kind': obj.kind,
            'id': str(obj.suggestion_id),
            'last_id': str(obj.last_id),
            'last_value': obj.last_value,
        })

    @classmethod
    def loads(cls, data):
        return cls(
            SuggestionId.parse(data['id']),
            SuggestionId.parse(data['last_id']),
            data['last_value'],
        )


Message.register_kind(PermGrant)


class Suggestion(Message):
    _kind = 'suggestion'

    def __init__(self, suggestion_id, value):
        self.suggestion_id = suggestion_id
        self.value = value

    @classmethod
    def dumps(cls, obj):
        return json.dumps({
            'kind': obj.kind,
            'suggestion_id': str(obj.suggestion_id),
            'value': obj.value,
        })

    @classmethod
    def loads(cls, data):
        return cls(
            SuggestionId.parse(data['suggestion_id']),
            data['last_value'],
        )


Message.register_kind(Suggestion)


class Accepted(Message):
    _kind = 'accepted'

    def __init__(self, suggestion_id):
        self.suggestion_id = suggestion_id

    @classmethod
    def dumps(cls, obj):
        return json.dumps({
            'kind': obj.kind,
            'suggestion_id': str(obj.suggestion_id),
        })

    @classmethod
    def loads(cls, data):
        return cls(
            SuggestionId.parse(data['suggestion_id']),
        )


Message.register_kind(Accepted)


class Nack(Message):
    _kind = 'nack'

    @classmethod
    def dumps(cls, obj):
        return json.dumps({
            'kind': obj.kind,
        })

    @classmethod
    def loads(cls, data):
        return cls()


Message.register_kind(Accepted)


class Proposer(object):

    def __init__(self, peers):
        self.peers = peers
        self.req_id = None
        self.value = None
        self.pending_permission_req = {}

    @property
    def can_propose(self):
        grants = [
            x for x in self.pending_permission_req
            if self.pending_permission_req[x]
        ]
        return len(grants) >= len(self.pending_permission_req) / 2 + 1


class Acceptor(object):
    pass


class Learner(object):
    pass



def parse_conf(name='conf'):
    conf = {}
    with io.open(name, 'r') as fp:
        lines = fp.readlines()
    for line in lines:
        line = line.strip()
        if line:
            name, ip, port = line.split(':')
            if name in conf:
                raise Exception("Multiple configs for name")
            conf[name] = (ip, int(port))
    return conf


class Client(object):

    def __init__(self, name, address, peers):
        self.name = name
        self.address = address
        self.peers = peers
        self.server = PeerServer(name, address, peers, self.handle_raw_message)
        self.client = PeerClient(name, peers, self.handle_raw_message)
        self.last_id = SuggestionId(0, '0')
        self.last_value = None

    def run(self):
        self.server.run()
        self.client.run()

    def connected(self):
        return self.server.connected() and self.client.connected()

    def suggest(self, value):
        self.proposer = Proposer(self.peers)
        global SERIAL
        sug_id = SuggestionId(SERIAL, self.name)
        SERIAL += 1
        self.proposer.perm_req = perm_req = PermReq(sug_id)
        self.proposer.value = value
        for peer in self.peers:
            self.client.send(peer, perm_req)
            self.proposer.pending_permission_req[peer] = None

    def suggestion_reply(self, msg_obj, conn, peer):
        log.error("suggest_reply %r %r %r",
            self.last_id, msg_obj.suggestion_id,
            self.last_id < msg_obj.suggestion_id
            )
        if self.last_id < msg_obj.suggestion_id:
            log.error("YES")
            grant = PermGrant(msg_obj.suggestion_id, self.last_id, self.last_value)
            self.last_id = msg_obj.suggestion_id
            send_msg(conn, Message.dumps(grant))
        else:
            log.error("NO")

    def grant_reply(self, msg_obj, conn, peer):
        log.error("grant_reply %r %r", msg_obj, peer.peer_name)
        if peer.peer_name not in self.proposer.pending_permission_req:
            log.error("Peer does not have pending permission request %s", peer.peer_name)
            return
        self.proposer.pending_permission_req[peer.peer_name] = True
        log.error("current replies %r", self.proposer.pending_permission_req)
        if self.proposer.can_propose:
            log.error("propose value")
            suggestion = Suggestion(self.proposer.req_id, self.proposer.value)
            for peer in self.peers:
                self.client.send(peer, suggestion)

    def handle_raw_message(self, raw_msg, conn, peer):
        msg_obj = Message.loads(raw_msg)
        if msg_obj.kind == 'permission_request':
            self.suggestion_reply(msg_obj, conn, peer)
        elif msg_obj.kind == 'permission_granted':
            self.grant_reply(msg_obj, conn, peer)




class PeerServer(object):

    def __init__(self, name, address, peers, handler):
        self.name = name
        self.address = address
        self.peers = peers
        self.lock = threading.Lock()
        self.connection_threads = {}
        self.lt = threading.Thread(target=self.listener)
        self.handler = handler

    def run(self):
        self.lt.start()

    @exception_logger
    def listener(self):
        sock = socket.socket()
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind(self.address)
        sock.listen(1)
        sock.settimeout(1)
        while True:
            try:
                conn, addr = sock.accept()
                raw_msg = None
                while raw_msg is None:
                    raw_msg = recv_msg(conn)
                msg = json.loads(raw_msg.decode('utf-8'))
                name = msg['name']
                if name not in self.peers:
                    log.error("Unknown peer %s %r %r", name, self.peers, msg)
                    conn.close()
                log.info("Got connection %s %r %r %r", name, conn, addr, msg)
                thread = threading.Thread(
                    target=self.connect_thread,
                    args=(name, conn)
                )
                thread.start()
                with self.lock:
                    self.connection_threads[name] = (thread, conn)
            except socket.timeout:
                pass

    @exception_logger
    def connect_thread(self, name, conn):
        send_msg(conn, json.dumps({'kind': 'connected'}).encode('utf-8'))
        while True:
            raw_msg = recv_msg(conn)
            if raw_msg:
                #msg = json.loads(raw_msg.decode('utf-8'))
                #log.error("Got msg %r", msg)
                self.handler(raw_msg, conn, self)

    def connected(self):
        with self.lock:
            return len(self.peers) ==  len(self.connection_threads)


class PeerClientConnection(object):

    def __init__(self, name, peer_name, addr, handler, _connected=False, _queue=None, _thread=None):
        self.name = name
        self.peer_name = peer_name
        self.addr = addr
        self.queue = _queue
        self.connected = _connected
        if self.queue is None:
            self.queue = queue.Queue()
        self.thread = _thread
        self.handler = handler
        if self.thread is None:
            self.thread = threading.Thread(
                target=self.peer_connection,
                args=(self.name, self.addr, self.queue, self.handler),
            )

    @exception_logger
    def peer_connection(self, name, addr, que, handler):
        sock = socket.socket()
        while True:
            try:
                sock.connect(addr)
            except ConnectionRefusedError:
                #log.error("Connection to %r refused", addr)
                time.sleep(1)
            else:
                break
        send_msg(sock, json.dumps({'name': self.name}).encode('utf-8'))
        raw = recv_msg(sock)
        if raw:
           msg = json.loads(raw.decode('utf-8'))
           self.connected = True
        while True:
            msg_obj = que.get()
            raw = Message.dumps(msg_obj)
            send_msg(sock, raw)
            raw = recv_msg(sock)
            handler(raw, socket, self)


class PeerClient(object):

    def __init__(self, name, peers, handler):
        self.name = name
        self.peers = peers
        self.lock = threading.Lock()
        self.peer_connections = {}
        self.handler = handler

    def run(self):
        for peer in self.peers:
            log.debug("Create client %s", peer)
            conn = PeerClientConnection(self.name, peer, self.peers[peer], self.handler)
            with self.lock:
                self.peer_connections[peer] = conn
            conn.thread.start()


    def connected(self):
        with self.lock:
            return len(self.peers) == len(
                [x.connected for x in self.peer_connections.values() if x.connected]
            )


    def send_all(self, msg_obj):
        for name in self.peer_connections:
            conn = self.peer_connections[name]
            conn.queue.put(msg_obj)

    def send(self, name, msg_obj):
        conn = self.peer_connections[name]
        conn.queue.put(msg_obj)



def send_msg(sock, msg):
    log.error("send_msg %r", msg)
    # Prefix each message with a 4-byte length (network byte order)
    msg = struct.pack('>I', len(msg)) + msg
    sock.sendall(msg)


def recvall(sock, n):
    # Helper function to recv n bytes or return None if EOF is hit
    data = b''
    while len(data) < n:
        try:
            packet = sock.recv(n - len(data))
        except BlockingIOError:
            return None
        if not packet:
            return None
        data += packet
    return data


def recv_msg(sock):
    # Read message length and unpack it into an integer
    raw_msglen = recvall(sock, 4)
    if not raw_msglen:
        return None
    msglen = struct.unpack('>I', raw_msglen)[0]
    # Read the message data
    return recvall(sock, msglen)


def main(name, conf):
    address = conf[name]
    client = Client(name, address, conf)
    client.run()
    while True:
        if client.connected():
            log.info("Client %s is connected", name)
            break
        time.sleep(1)
    time.sleep(3)
    if client.name == 'a':
        log.error("SUGGEST")
        client.suggest('foo')
    while True:
        time.sleep(10)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    name = sys.argv[-1]
    conf = parse_conf()
    main(name, conf)
