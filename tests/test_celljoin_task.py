"""Celljoin fat tasks and content-addressed scheduling without a cluster."""
import asyncio
from types import SimpleNamespace
import pytest
from seamless import Buffer, Checksum, CacheMissError
from seamless.checksum import celljoin as joins, expression as expressions
from seamless.error_envelope import encode_error
from seamless_dask import client
from seamless_remote import buffer_remote


@pytest.fixture(autouse=True)
def clean(monkeypatch):
    expressions.get_expression_cache().clear()
    monkeypatch.setattr(client, '_run_on_worker_loop', lambda factory: asyncio.run(factory()))
    yield
    expressions.get_expression_cache().clear()


def definition(member):
    buffer = joins.celljoin_buffer(joins.build_celljoin(None, {'a': member}))
    buffer.tempref()
    return buffer


def payload(buffer, scratch=True, celltype='plain'):
    return {'celljoin_checksum': buffer.get_checksum().hex(), 'celltype': celltype, 'scratch': scratch}


def test_fat_definition_error_is_forwarded_unchanged():
    error = encode_error(CacheMissError(Checksum('fa' * 32)))
    value = ('fa' * 32, None, error)
    assert client._celljoin_task({'celltype': 'plain', 'scratch': True}, value) == value


@pytest.mark.parametrize('scratch', [True, False])
def test_fat_result_and_publication_follow_scratch(monkeypatch, scratch):
    member = Buffer(43, 'plain')
    member.tempref()
    held = definition(member.get_checksum())
    writes = []
    async def write(checksum, buffer):
        writes.append((checksum, buffer.get_value('plain')))
        return True
    monkeypatch.setattr(buffer_remote, 'write_buffer', write)
    checksum, buffer, error = client._celljoin_task(payload(held, scratch), (held.get_checksum().hex(), held, None))
    result = Buffer({'a': 43}, 'plain').get_checksum()
    assert error is None
    assert checksum == result.hex()
    assert isinstance(buffer, Buffer)
    assert buffer.get_value('plain') == {'a': 43}
    assert writes == ([] if scratch else [(result, {'a': 43})])


def test_member_cache_miss_is_encoded():
    missing = Checksum('fb' * 32)
    held = definition(missing)
    _, _, envelope = client._celljoin_task(payload(held), (held.get_checksum().hex(), held, None))
    assert envelope['error']['kind'] == 'cache_miss'
    assert envelope['error']['checksum'] == missing.hex()


@pytest.mark.parametrize('celltype', ['deepcell', 'deepfolder'])
def test_deep_dispatch_is_refused(celltype):
    held = definition(Checksum('fc' * 32))
    _, _, envelope = client._celljoin_task(payload(held, celltype=celltype), (held.get_checksum().hex(), held, None))
    assert envelope['error']['kind'] == 'expression_evaluation'


def test_future_key_deduplicates_equal_payload_and_distinguishes_scratch():
    submissions = []
    class Scheduler:
        def submit(self, function, payload, definition_future, **kwargs):
            submissions.append((function, payload, definition_future, kwargs))
            return kwargs['key']
    subject = SimpleNamespace(_client=Scheduler())
    future = SimpleNamespace(key='fat-definition')
    request = {'celljoin_checksum': '12' * 32, 'celltype': 'plain', 'scratch': True}
    first = client.SeamlessDaskClient.get_celljoin_future(subject, request, future)
    equal = client.SeamlessDaskClient.get_celljoin_future(subject, dict(request), future)
    materialized = client.SeamlessDaskClient.get_celljoin_future(subject, {**request, 'scratch': False}, future)
    assert first == equal
    assert first != materialized
    assert first.startswith('celljoin-')
    assert submissions[0][0] is client._celljoin_task
    assert submissions[0][1] == request
    assert submissions[0][2] is future
