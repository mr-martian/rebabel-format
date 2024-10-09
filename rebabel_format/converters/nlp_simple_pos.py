#!/usr/bin/env python3

from rebabel_format.parameters import Parameter
from rebabel_format.reader import LineReader


class NLPSimplePartOfSpeechReader(LineReader):
    """Read in a text file.
    Each line consists of a single sentence made up of words tagged with their parts of speech.
    Each word is separated from its part of speech by a delimiter, by default "/".
    The words are separated from each other by spaces.

    Example file structure:

    The/DET dog/NOUN barked/VERB ./PUNC
    The/DET cat/NOUN meowed/VERB ./PUNC
    """

    identifier = "nlp_simple_pos"
    delimiter = Parameter(default="/", type=str)
    include_boundaries = True

    def is_boundary(self, line):
        return True

    def process_line(self, line: str):
        """ Process a sentence one word/part of speech pair at a time.

        Positional arguments:
        line -- A sentence made up of word/part of speech pairs. A word and its
        part of speech are separated by a delimiter (ex: jumped/VERB), and
        word/part of speech pairs are separated by spaces.
        """

        for index, word_part_of_speech_pair in enumerate(line.strip().split(), 1):
            word, part_of_speech = word_part_of_speech_pair.split(self.delimiter)

            self.set_type(index, "word")
            self.set_parent(index, "sentence")
            self.set_feature(index, "meta:index", "int", index)
            self.set_feature(index, "nlp:form", "str", word)
            self.set_feature(index, "nlp:pos", "str", part_of_speech)

        if line:
            self.set_type('sentence', 'sentence')
            self.set_feature('sentence', 'meta:index', 'int', self.block_count+1)
