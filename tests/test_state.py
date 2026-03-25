from dataclasses import dataclass
from pytest import fixture, mark
from pytest_asyncio import fixture as async_fixture

from polyjuice.state import OutOfSync, StateChange, compute_state


@dataclass
class TestState:
    a: int
    b: int
    c: int
    d: int

    def clone(self):
        return TestState(self.a, self.b, self.c, self.d)


@fixture
def state():
    return TestState(0, 0, 0, 0)


@fixture
def change(state):
    def _change(a=None, b=None, c=None, d=None):
        def execute():
            if a is not None:
                state.a = a
            if b is not None:
                state.b = b
            if c is not None:
                state.c = c
            if d is not None:
                state.d = d

        return execute

    return _change


@fixture
def clock():
    async def _clock():
        for i in range(100):
            yield i

    return _clock


@fixture
def stream_1(change):
    async def _stream_1():
        yield StateChange(1, change(a=1))
        yield StateChange(1.4, change(b=1))
        yield StateChange(2, change(a=0, c=2))
        yield StateChange(3.1, change(a=3, c=1))

    return _stream_1


@fixture
def stream_2(change):
    async def _stream_2():
        yield StateChange(2.6, change(d=2))
        yield StateChange(2.5, change(d=1))
        yield StateChange(4, change(d=3))
        yield StateChange(9.1, change(d=5))
        yield StateChange(8.1, change(d=42))

    return _stream_2


@fixture
def stream_3(change):
    async def _stream_3():
        yield StateChange(3, change(a=4))
        yield StateChange(1, change(b=2, c=1))

    return _stream_3


@async_fixture
async def output_states(state, clock, stream_1, stream_2):
    states = []
    async for timestamp, stats in compute_state(clock(), stream_1(), stream_2()):
        states.append((timestamp, stats, state.clone()))
    return states


@mark.asyncio
async def test_stop(output_states):
    assert output_states[-1][0] == 10


@mark.asyncio
async def test_clock(output_states, clock):
    assert len(output_states) == output_states[-1][0] + 1
    i = 0
    async for timestamp in clock():
        if i >= len(output_states):
            return
        assert output_states[i][0] == timestamp
        i += 1


@mark.asyncio
async def test_state_values(output_states):
    values = [
        (0, 0, 0, 0),
        (1, 0, 0, 0),
        (0, 1, 2, 0),
        (0, 1, 2, 2),
        (3, 1, 1, 3),
        (3, 1, 1, 3),
        (3, 1, 1, 3),
        (3, 1, 1, 3),
        (3, 1, 1, 3),
        (3, 1, 1, 42),
        (3, 1, 1, 5),
    ]
    for i, v in enumerate(values):
        assert output_states[i][2] == TestState(*v)


@mark.asyncio
async def test_out_of_sync(clock, stream_2, stream_3):
    exception_raised = False
    try:
        async for _, _ in compute_state(clock(), stream_2(), stream_3()):
            pass
    except OutOfSync:
        exception_raised = True
    assert exception_raised


@mark.asyncio
async def test_reordering(state, clock, stream_3):
    output_states = []
    async for timestamp, stats in compute_state(clock(), stream_3(), buffer_length=2):
        output_states.append((timestamp, stats, state.clone()))
    assert output_states[1][2] == TestState(0, 2, 1, 0)
    assert output_states[3][2] == TestState(4, 2, 1, 0)
