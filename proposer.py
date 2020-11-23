from pax import PermReq, SuggestionId


class Proposer(object):

    def __init__(self, name, acceptors, serial=0):
        self.name = name
        self.acceptors = acceptors
        self.serial = serial


    def propose(self, value):
        sug_id = SuggestionId(self.serial, self.name)
        self.serial += 1
        perm_req = PermReq(sug_id)

        for acceptor in self.acceptors:
            acceptor.
        # Create grant

        # Grant reply

        # Propose value
