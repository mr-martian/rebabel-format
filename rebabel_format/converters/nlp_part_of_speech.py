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

    def reset(self):
        """placeholder docstring"""
        self.word_idx = 0

    def end(self):
        """placeholder docstring"""
        if self.id_seq:
            self.set_type("sentence", "sentence")
            self.set_feature("sentence", "meta:index", "int", self.block_count)
        super().end()

    def process_line(self, line: str):
        """Process one word, part of speech pair at a time.

        Positional arguments:
        line -- a word and its part of speech separated by a delimiter (ex: jumped/VERB)
        """
        split_line = line.strip().split(" ")

        for index, word_part_of_speech_pair in enumerate(split_line):
            word, part_of_speech = word_part_of_speech_pair.split(self.delimiter)

            index_as_string = str(index + 1)

            self.set_type(index_as_string, "word")
            self.set_parent(index_as_string, "sentence")
            self.set_feature(index_as_string, "UD:id", "str", index_as_string)

            self.word_idx += 1

            self.set_feature(index_as_string, "meta:index", "int", self.word_idx)
            self.set_feature(index_as_string, "UD:form", "str", word)
            self.set_feature(index_as_string, "UD:upos", "str", part_of_speech)
