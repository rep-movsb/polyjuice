from dataclasses import dataclass
from lzma import open as lzma_open
from logging import info
from os.path import basename
from re import match

from polyjuice.data.stream import Stream, StreamReader


@dataclass
class RawStreamReader(StreamReader):

    filename: str
    n: int
    total: int

    def read(self):
        with lzma_open(self.filename, "rb") as input:
            total_lines = 0
            for line in input:
                yield line
                total_lines += 1
        info(f"{self.n / self.total:.2f} {total_lines:<8} {self.filename}")


class RawStream(Stream):

    def __init__(self, input_filenames: list[str]):
        self.input_filenames = input_filenames

    def readers(self, start_timestamp: int | None):
        if start_timestamp is not None:
            timestamp_to_file_map = {
                float(
                    match(r"^([0-9]+\.[0-9]+).*$", basename(filename)).group(1)
                ): filename
                for filename in self.input_filenames
            }
            files_to_read = [
                filename
                for (timestamp, filename) in timestamp_to_file_map.items()
                if timestamp > start_timestamp
            ]
        else:
            files_to_read = self.input_filenames

        return [
            RawStreamReader(filename, i, len(files_to_read))
            for i, filename in enumerate(sorted(files_to_read))
        ]
