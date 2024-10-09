#!/usr/bin/env python3

from rebabel_format.parameters import Parameter
from rebabel_format.reader import LineReader


class NLPTwoFilePartOfSpeechReader(LineReader):
    """Read in a text file.
    Both a sentence file and a part of speech file need to be uploaded in two different imports.
    Either the sentence or the part of speech file can be uploaded first, but in either case
    the second import needs to include the merge_on parameter.
    What a line consists of depends on the type of file. For a sentence file, each line is a sentence
    made up of words and punctuation separated by spaces. For a part of speech file, each line
    is made up of parts of speech separated by spaces and corresponding to the words and punctuation
    from the sentence file.

    Example file structure:

    Sentence File -
    The dog barked .
    The cat meowed .

    Part of Speech File -

    DET NOUN VERB PUNC
    DET NOUN VERB PUNC
    """

    identifier = "nlp_two_file_pos"
    include_boundaries = True
    isPartOfSpeechFile = Parameter(type=bool)

    if isPartOfSpeechFile:
        feature = "nlp:pos"
    else:
        feature = "nlp:form"

    def is_boundary(self, line):
        return True

    def process_line(self, line: str):
        """Process a sentence one word or part of speech at a time

        Positional arguments:
        line -- A sentence made up of words separated by spaces,
        or a line of parts of speech separated by spaces
        """

        for index, string in enumerate(line.strip().split(), 1):
            self.set_type(index, "word")
            self.set_parent(index, "sentence")
            self.set_feature(index, "meta:index", "int", index)
            self.set_feature(index, self.feature, "str", string)

        if line:
            self.set_type('sentence', 'sentence')
            self.set_feature('sentence', 'meta:index', 'int', self.block_count + 1)
