#!/usr/bin/env python3

from rebabel_format.parameters import Parameter
from rebabel_format.reader import LineReader


class NLPTwoFilePartOfSpeechReader(LineReader):
    """

    """

    identifier = "nlp_two_file_pos"
    delimiter = Parameter(default="/", type=str)
    isPartOfSpeechFile = Parameter(type=bool)

    if isPartOfSpeechFile:
        feature = "UD:pos"
    else:
        feature = "UD:form"

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
        """
        """
        split_line = line.strip().split(" ")

        for index, string in enumerate(split_line):
            index_as_string = str(index + 1)

            self.set_type(index_as_string, "word")
            self.set_parent(index_as_string, "sentence")
            self.set_feature(index_as_string, "UD:id", "str", index_as_string)

            self.word_idx += 1

            self.set_feature(index_as_string, "meta:index", "int", self.word_idx)
            self.set_feature(index_as_string, self.feature, "str", string)
