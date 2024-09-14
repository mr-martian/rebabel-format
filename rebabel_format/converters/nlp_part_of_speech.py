#!/usr/bin/env python3

from rebabel_format.reader import LineReader

class NLPPartOfSpeechReader(LineReader):
    """Read in a text file consisting of lines with a line number and a single word and its part of speech, separated by the ↑ delimiter.
    
    Example file structure:
    
    1 The↑DET
    2 dog↑NOUN
    3 barked↑VERB
    4 .↑PUNC
    """
    
    identifier = 'nlp_pos'
       
    def reset(self):
        self.word_idx = 0
          
    def end(self):
        if self.id_seq:
            self.set_type('sentence', 'sentence')
            self.set_feature('sentence', 'meta', 'index', 'int',
                             self.block_count)
        super().end()

    def process_line(self, line: str):
        """Process one word, part of speech pair at a time.
        
        Positional arguments:
        line -- a word and its part of speech separated by the ↑ delimiter (ex: jumped↑VERB)
        """
        split_line = line.strip().split(' ')
        
        line_number = split_line[0]
        word, part_of_speech = split_line[1].split("↑")
        
        self.set_type(line_number, 'word')
        self.set_parent(line_number, 'sentence')
        self.set_feature(line_number, 'UD', 'id', 'str', line_number)
        
        self.word_idx += 1
        
        self.set_feature(line_number, 'meta', 'index', 'int', self.word_idx)
        self.set_feature(line_number, 'UD', 'form', 'str', word)
        self.set_feature(line_number, 'UD', 'upos', 'str', part_of_speech)