#!/usr/bin/env python3

from rebabel_format.process import Process
from rebabel_format.parameters import Parameter, MappingParameter, UsernameParameter

class Importer(Process):
    '''Read the contents of one or more files into the database'''

    name = 'import'
    mode = Parameter(type=str, help='the format of the file(s)')
    username = UsernameParameter(help='the username for the imported data')
    infiles = Parameter(type=list, help='the paths to the files')
    glob = Parameter(type=bool, default=False, help='whether to perform glob expansion on the file names')
    mappings = MappingParameter(required=False, help='feature and type remappings')

    def run(self):
        from rebabel_format.reader import ALL_READERS, ReaderError
        if self.mode not in ALL_READERS:
            raise ValueError(f'Unknown reader {self.mode}.')
        reader = ALL_READERS[self.mode](self.db, self.username,
                                        self.conf, self.other_args)
        reader.set_mappings(*self.mappings)
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

    @classmethod
    def help_text_epilog(cls):
        from rebabel_format.reader import ALL_READERS, ReaderError
        readers = '\n- '.join(sorted(ALL_READERS.keys()))
        return f'''The following readers are available:
- {readers}

To view further information about one of them, you can run
`rebabel-format help import.[reader]`
e.g.
`rebabel-format help import.conllu`

This will include a description of what the unit types and feature
names of the imported data will be.

These names can be overridden with the `mappings` parameter such
as the following:

[[mappings]]
in_tier = "morph"
in_feature = "POS"
out_tier = "UD"
out_feature = "upos"

This will cause any feature that would have been named `UD:upos`
to instead be named `morph:POS`. A similar approach can be used
to rename unit types:

[[mappings]]
in_type = "token"
out_type = "word"

This will cause item that would otherwise have been imported as
a `word` to be imported as a `token`. If a mapping entry specifies
both types and features, it will be interpreted as only renaming
the feature in question when applied to that type.
'''
