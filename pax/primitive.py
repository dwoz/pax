import collections
import math


class SuggestionId(collections.UserString):


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


class PendingPermissionRequest:

    def __init__(self, peer_name, granted=False):
        self.peer_name = peer_name
        self.granted = granted

    def __repr__(self):
        return "<PendingPermissionRequest(peer_name={}, granted={})>".format(
            self.peer_name, self.granted
        )


class Proposer:

    def __init__(self, peers, req_id, value):
        self.peers = peers
        self.req_id = req_id
        self.value = value
        self.pending_permission_req = {name:False for name in self.peers}
        self.pending_proposal_req = {name:False for name in self.peers}

    def grant(self, name):
        if name not in self.pending_permission_req:
            raise Exception("Not a valid peer name")
        self.pending_permission_req[name] = True

    def accept(self, name):
        if name not in self.pending_proposal_req:
            raise Exception("Not a valid peer name")
        self.pending_proposal_req[name] = True

    @property
    def can_propose(self):
        grants = [
            x for x in self.pending_permission_req
            if self.pending_permission_req[x]
        ]
        return len(grants) >= math.floor(len(self.pending_permission_req) / 2) + 1

    @property
    def value_accepted(self):
        proposals = [
            x for x in self.pending_proposal_req
            if self.pending_proposal_req[x]
        ]
        return len(proposals) >= math.floor(len(self.pending_proposal_req) / 2) + 1


class Acceptor(object):

    def __init__(self, last_id, last_value, grant_id=None):
        self.last_id = last_id
        self.last_value = last_value
        self.grant_id = grant_id

    def should_grant(self, sug_id):
        return self.last_id < sug_id

    def should_accept(self, sug_id):
        if self.grant_id is None:
            raise Exception("Cannot accept when no grant set")
        return self.grant_id == sug_id

    def grant(self, sug_id):
        if not self.should_grant(sug_id):
            raise Exception("Cannot grant that id")
        self.grant_id = sug_id

    def accept(self, sug_id, value):
        if self.grant_id is None:
            raise Exception("Cannot accept without grant")
        if self.grant_id != sug_id:
            raise Exception("Suggestion id is not a valid grant id")
        self.last_id = sug_id
        self.value = value


class Learner(object):

    def __init__(self, value=None):
        self.value = value

    def learn(self, value):
        self.value = value
