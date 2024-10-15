#!/usr/bin/env python3

from rebabel_format.parameters import Parameter
from rebabel_format.reader import LineReader


class NLPPartOfSpeechReader(LineReader):
    """
    Supports two different methods for importing part of speech data.

    First method:
        Import one text file, with a "combined" nlpFileType, where each line consists of a single sentence
        made up of words tagged with their parts of speech.
        Each word is separated from its part of speech by a delimiter, by default "/".
        The word/pos pairs are separated from each other by spaces.

        Example file structure:

        The/DET dog/NOUN barked/VERB ./PUNC
        The/DET cat/NOUN meowed/VERB ./PUNC

    Second method:
        Import two text files, one with the original language data and one with the parts of speech.
        The language data file (with a "language" nlpFileType) and the part of speech file
        (with a "pos" nlpFileType) need to be uploaded in two different imports.
        Either the language data or the part of speech file can be uploaded first, but in either case
        the second import needs to include the merge_on parameter.
        What a line consists of depends on the type of file. For a language data file, each line is a sentence
        made up of words and punctuation separated by spaces. For a part of speech file, each line
        is made up of parts of speech separated by spaces, corresponding to the words and punctuation
        from the language data file.

        Example file structure:

        Language Data File -
        The dog barked .
        The cat meowed .

        Part of Speech File -

        DET NOUN VERB PUNC
        DET NOUN VERB PUNC
    """

    identifier = "nlp_pos"
    include_boundaries = True
    delimiter = Parameter(default="/", type=str)
    nlpFileType = Parameter(type=str, choices=["language", "pos", "combined"])

    def is_boundary(self, line):
        return True

    def process_line(self, line: str):
        """Process one line of the file at a time."""

        for index, item in enumerate(line.strip().split(), 1):
            self.set_type(index, "word")
            self.set_parent(index, "sentence")
            self.set_feature(index, "meta:index", "int", index)

            if self.nlpFileType == "combined":
                word, part_of_speech = item.split(self.delimiter)
                self.set_feature(index, "nlp:form", "str", word)
                self.set_feature(index, "nlp:pos", "str", part_of_speech)
            elif self.nlpFileType == "language":
                self.set_feature(index, "nlp:form", "str", item)
            else:
                self.set_feature(index, "nlp:pos", "str", item)

        if line:
            self.set_type('sentence', 'sentence')
            self.set_feature('sentence', 'meta:index', 'int', self.block_count)
