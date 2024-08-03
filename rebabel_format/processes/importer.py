#!/usr/bin/env python3

from .process import Process
from .parameters import Parameter, UsernameParameter

class Importer(Process):
    name = 'import'
    mode = Parameter(type=str)
    username = UsernameParameter()
    infiles = Parameter(type=list)

    def run(self):
        from .. import converters
        from ..converters.reader import ALL_READERS
        if self.mode not in ALL_READERS:
            raise ValueError(f'Unknown reader {self.mode}.')
        reader = ALL_READERS[self.mode](self.db, self.username)
        import time
        for pth in self.infiles:
            start = time.time()
            fin = reader.open_file(pth)
            reader.read_file(fin)
            reader.close_file(fin)
            self.logger.info(f"Read '{pth}' in {time.time()-start} seconds.")
