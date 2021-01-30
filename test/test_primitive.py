from pax.primitive import SuggestionId, Proposer, Acceptor, Learner

import pytest


@pytest.mark.parametrize('sug_1_args,sug_2_args,op', [
    (('1', 'a'), ('1', 'b'), '<'),
    (('1', 'b'), ('1', 'b'), '=='),
    (('1', 'b'), ('1', 'a'), '>'),
])
def test_suggestion_id_comparison(sug_1_args, sug_2_args, op):
    result = eval("SuggestionId('{}', '{}') {} SuggestionId('{}', '{}')".format(
        sug_1_args[0],
        sug_1_args[1],
        op,
        sug_2_args[0],
        sug_2_args[1]
    ))
    assert result is True


def test_suggestion_id_to_str():
    assert str(SuggestionId('1', 'a')) == '1,a'


def test_suggestion_id_parse():
    sug = SuggestionId.parse('1,a')
    assert sug.uid == 1
    assert sug.node == 'a'


def test_proposer_grant():
    config = {
        'a': ('127.0.0.1', 55501),
        'b': ('127.0.0.1', 55502),
        'c': ('127.0.0.1', 55503),
    }
    sug_id = SuggestionId(1, 'a')
    proposer = Proposer(config, sug_id, 'test value')
    assert proposer.can_propose == False
    proposer.grant('a')
    proposer.grant('b')
    assert proposer.can_propose == True


def test_proposer_accept():
    config = {
        'a': ('127.0.0.1', 55501),
        'b': ('127.0.0.1', 55502),
        'c': ('127.0.0.1', 55503),
    }
    sug_id = SuggestionId(1, 'a')
    proposer = Proposer(config, sug_id, 'test value')

    assert proposer.value_accepted is False
    proposer.accept('a')
    proposer.accept('b')
    assert proposer.value_accepted is True


def test_acceptor_should_grant():
    acceptor = Acceptor(SuggestionId(0, '0'), None)
    sug = SuggestionId(0, 'a')
    assert acceptor.should_grant(sug) is True


def test_acceptor_should_not_grant():
    acceptor = Acceptor(SuggestionId(0, '0'), None)
    sug = SuggestionId(0, '0')
    assert acceptor.should_grant(sug) is False


def test_acceptor_should_accept():
    acceptor = Acceptor(SuggestionId(0, '0'), None, SuggestionId(0, 'a'))
    sug = SuggestionId(0, 'a')
    assert acceptor.should_accept(sug) is True


def test_acceptor_should_not_accept_before_grant():
    acceptor = Acceptor(SuggestionId(0, '0'), None)
    assert acceptor.last_value is None
    assert acceptor.grant_id is None
    sug = SuggestionId(0, '0')
    with pytest.raises(Exception):
        assert acceptor.should_accept(sug)


def test_acceptor_should_not_accept_after_grant():
    acceptor = Acceptor(SuggestionId(0, '0'), None, SuggestionId(0, 'b'))
    # Grant id is grater than id to be accepted
    sug = SuggestionId(0, 'a')
    assert acceptor.should_accept(sug) is False


def test_acceptor_grant():
    acceptor = Acceptor(SuggestionId(0, '0'), None)
    sug = SuggestionId(0, 'a')
    acceptor.grant(sug)
    acceptor.grant_id == sug


def test_paxos():
    config = {
        'a': ('127.0.0.1', 55501),
        'b': ('127.0.0.1', 55502),
        'c': ('127.0.0.1', 55503),
    }
    sug_id = SuggestionId(0, '0')
    acceptors = {k:Acceptor(sug_id, None) for k in config}
    sug_id = SuggestionId(1, 'a')
    value = 'meh'
    proposer = Proposer(
        config,
        sug_id,
        value,
    )
    learner = Learner()
    for a in proposer.peers:
        assert acceptors[a].should_grant(sug_id)
        acceptors[a].grant(sug_id)
    for a in proposer.peers:
        assert acceptors[a].should_accept(sug_id)
        acceptors[a].accept(sug_id, value)
    learner.learn(value)
    assert learner.value == value
