#!/usr/bin/env python3

from .process import Process
from .parameters import Parameter

class Export(Process):
    name = 'export'
    mode = Parameter(type=str)
    outfile = Parameter(type=str)

    def run(self):
        from .. import converters
        from ..converters.writer import ALL_WRITERS
        if self.mode not in ALL_WRITERS:
            raise ValueError(f'Unknown writer {self.mode}.')
        writer = ALL_WRITERS[self.mode](self.db, self.conf)
        with open(self.outfile, 'w') as fout:
            writer.write(fout)
