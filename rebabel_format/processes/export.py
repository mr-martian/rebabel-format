#!/usr/bin/env python3

from rebabel_format.process import Process
from rebabel_format.parameters import Parameter, MappingParameter

class Export(Process):
    '''Output the contents of the database in a particular format'''

    name = 'export'
    mode = Parameter(type=str, help='the format to output')
    outfile = Parameter(type=str, help='the file to output to')
    mappings = MappingParameter(required=False, help='feature and type remappings')

    def run(self):
        from rebabel_format.writer import ALL_WRITERS
        if self.mode not in ALL_WRITERS:
            raise ValueError(f'Unknown writer {self.mode}.')
        writer = ALL_WRITERS[self.mode](self.db,
                                        self.mappings[0], self.mappings[1],
                                        self.conf, self.other_args)
        with open(self.outfile, 'w') as fout:
            writer.write(fout)
