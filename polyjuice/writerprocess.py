from contextlib import contextmanager
from logging import warning
from multiprocessing import Process, JoinableQueue
from queue import Empty, Full
from random import choice
from typing import Any, Callable

queues_stack = []


@contextmanager
def io_queues(max_size=0):
    input_queue = JoinableQueue(maxsize=max_size)
    exception_queue = JoinableQueue()
    queues_stack.append((input_queue, exception_queue))
    try:
        yield (input_queue, exception_queue, len(queues_stack) - 1)
    finally:
        queues_stack.pop()


class WriterProcessException(Exception): ...


class WriterProcessHandler:

    stack_index: int
    queue_was_joined: bool

    def __init__(self, stack_index: int):
        self.stack_index = stack_index
        self.queue_was_joined = False

    def write(self, chunk: Any, block=True):
        global queues_stack
        input_queue, exception_queue = queues_stack[self.stack_index]
        try:
            exception = exception_queue.get_nowait()
            raise WriterProcessException() from exception
        except Empty:
            pass
        input_queue.put({"payload": chunk}, block)

    def join(self):
        global queues_stack
        input_queue, _ = queues_stack[self.stack_index]
        input_queue.join()
        self.queue_was_joined = True


@contextmanager
def writer_process(target: Callable[[Any], None], queue_max_size=0):
    with io_queues(queue_max_size) as (input_queue, exception_queue, stack_index):

        def receive():
            try:
                while (packet := input_queue.get()) is not None:
                    yield packet["payload"]
                    input_queue.task_done()
            except () as ex:
                exception_queue.put(ex)
                raise

        process = Process(target=target, args=(receive(),))
        process.start()

        handler = WriterProcessHandler(stack_index)

        try:
            yield handler
        finally:
            if process.is_alive():
                input_queue.put(None)
                if not handler.queue_was_joined:
                    warning("Queue was not joined, potential queue corruption")
                    process.join(15000)
                    process.terminate()
                else:
                    process.join()


def pool_write(writers: list[WriterProcessHandler], chunk: Any):
    for writer in writers:
        try:
            writer.write(chunk, block=False)
            return
        except Full:
            pass
    choice(writers).write(chunk)
