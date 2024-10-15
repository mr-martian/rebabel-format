#!/usr/bin/env python3

from rebabel_format.parameters import Parameter
from rebabel_format.reader import LineReader


class NLPPartOfSpeechReader(LineReader):
    """Read in a text file.
    Each line consists of a single sentence made up of words tagged with their parts of speech.
    Each word is separated from its part of speech by a delimiter, by default "/".
    The words are separated from each other by spaces.

    Example file structure:

    The/DET dog/NOUN barked/VERB ./PUNC
    The/DET cat/NOUN meowed/VERB ./PUNC
    """

    identifier = "nlp_pos"
    delimiter = Parameter(default="/", type=str)
    include_boundaries = True

    def is_boundary(self, line):
        return True

    def process_line(self, line: str):
        """Process one word, part of speech pair at a time.

        Positional arguments:
        line -- a word and its part of speech separated by a delimiter (ex: jumped/VERB)
        """

        for index, item in enumerate(line.split(), 1):
            pieces = item.split(self.delimiter)

            self.set_type(index, "word")
            self.set_parent(index, "sentence")
            self.set_feature(index, "meta:index", "int", index)
            self.set_feature(index, "nlp:form", "str", pieces[0])
            if len(pieces) > 1:
                self.set_feature(index, "nlp:pos", "str", pieces[1])

        if line:
            self.set_type('sentence', 'sentence')
            self.set_feature('sentence', 'meta:index', 'int', self.block_count)
