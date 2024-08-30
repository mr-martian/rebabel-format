#!/usr/bin/env python3

from .process import Process
from .parameters import Parameter, MappingParameter

class Export(Process):
    '''Output the contents of the database in a particular format'''

    name = 'export'
    mode = Parameter(type=str, help='the format to output')
    outfile = Parameter(type=str, help='the file to output to')
    mappings = MappingParameter(required=False, help='feature and type remappings')

    def run(self):
        from .. import converters
        from ..converters.writer import ALL_WRITERS
        if self.mode not in ALL_WRITERS:
            raise ValueError(f'Unknown writer {self.mode}.')
        writer = ALL_WRITERS[self.mode](self.db, self.conf,
                                        type_map=self.mappings[0],
                                        feat_map=self.mappings[1])
        with open(self.outfile, 'w') as fout:
            writer.write(fout)
