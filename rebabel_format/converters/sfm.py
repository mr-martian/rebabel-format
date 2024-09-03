from rebabel_format.reader import LineReader
import re

class SFMReader(LineReader):
    '''
    Each block of lines in the file will be imported as `sentence`.
    Elements in \\tx will be imported as `word`, and elements in `\\mb`
    and `\\gl` will be imported as `morpheme`. Both `word` and `morpheme`
    will have the integer feature `meta:index`.

    The following markers are understood (all string features):

    \\ref -> sentence SFM:reference

    \\tx -> word SFM:form

    \\mb -> morpheme SFM:form

    \\gl -> morpheme SFM:gls

    \\ft -> sentence SFM:translation
    '''
    identifier = 'sfm'
    short_name = 'SFM'
    long_name = 'Standard Format Markers'

    block_name = 'sentence'

    morph_sep = re.compile(r'(?=[-=])')

    def is_boundary(self, line):
        return line.startswith(r'\ref')

    def iter_morphs(self, line):
        for wid, word in enumerate(line[3:].strip().split('&'), 1):
            for mid, morph in enumerate(self.morph_sep.split(word)):
                yield (morph.strip(), wid, mid)

    def process_line(self, line):
        if not line:
            return

        cmd = line.split()[0]
        if cmd == r'\ref':
            self.set_type('sentence', 'sentence')
            self.set_feature('sentence', 'SFM', 'reference', 'str',
                             line.split('.')[1])

        elif cmd == r'\tx':
            for wid, word in enumerate(line.split()):
                if wid == 0:
                    continue
                name = ('word', wid)
                self.set_type(name, 'word')
                self.set_parent(name, 'sentence')
                self.set_feature(name, 'SFM', 'form', 'str', word)
                self.set_feature(name, 'meta', 'index', 'int', wid)

        elif cmd == r'\mb':
            for morph, wid, mid in self.iter_morphs(line):
                name = ('morph', wid, mid)
                self.set_type(name, 'morpheme')
                self.set_parent(name, ('word', wid))
                self.set_feature(name, 'meta', 'index', 'int', mid)
                self.set_feature(name, 'SFM', 'form', 'str', morph)

        elif cmd == r'\gl':
            for morph, wid, mid in self.iter_morphs(line):
                name = ('morph', wid, mid)
                self.set_feature(name, 'SFM', 'gls', 'str', morph)

        elif cmd == r'\ft':
            self.set_feature('sentence', 'SFM', 'translation', 'str',
                             line[3:].strip())
