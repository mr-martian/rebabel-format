#!/usr/bin/env python3

from .process import Process
from .parameters import Parameter, UsernameParameter

class Importer(Process):
    name = 'import'
    mode = Parameter(type=str)
    username = UsernameParameter()
    infiles = Parameter(type=list)
    glob = Parameter(type=bool, default=False)

    def run(self):
        from .. import converters
        from ..converters.reader import ALL_READERS, ReaderError
        if self.mode not in ALL_READERS:
            raise ValueError(f'Unknown reader {self.mode}.')
        reader = ALL_READERS[self.mode](self.db, self.username)
        import time
        fnames = self.infiles
        if self.glob:
            import glob
            import itertools
            fnames = itertools.chain.from_iterable(
                map(lambda fname: sorted(glob.glob(fname)), self.infiles)
            )
        for pth in fnames:
            start = time.time()
            try:
                reader.read(pth)
                self.logger.info(f"Read '{pth}' in {time.time()-start} seconds.")
            except ReaderError:
                self.logger.error(f"Import of '{pth}' failed.")
