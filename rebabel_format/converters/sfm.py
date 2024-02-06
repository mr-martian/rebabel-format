#!/usr/bin/env python3

from .reader import Reader
import re

class SFMReader(Reader):
    identifier = "sfm"
    short_name = "SFM"
    long_name = "Standard Format Markers"

    def read_file(self, fin):
        self.ensure_feature('word','SFM','index','int')
        cur = []
        for line in fin:
            if not line.strip():
                continue
            elif line.startswith('\\ref'):
                self.process_sentence(cur)
                cur = []
                cur.append(line.rstrip())
            else:
                cur.append(line.rstrip())
        if cur:
            self.process_sentence(cur)

    def process_sentence(self, lines):
        sent_id = self.create_unit('sentence')
        words = []
        morphemes = []
        idx = 0
        mix = 0
        for l in lines:
            if l.startswith('\\ref'):
                ref = l.split('.')[1]
                self.ensure_feature('sentence', 'SFM', 'reference', 'str')
                self.set_feature(sent_id, 'SFM', 'reference', ref)
            elif l.startswith('\\tx'):
                wds = l[3:].strip().split()
                for word in wds:
                    word = word.strip()
                    wid = self.create_unit('word', parent=sent_id)
                    words.append(wid)
                    self.ensure_feature('word', 'sentence', 'form', 'str')
                    self.set_feature(wid, 'sentence', 'form', word)
            elif l.startswith('\\mb'):
                breaks = l.replace('\\mb','').strip().split('&')
                for mb in breaks:
                    mid = self.create_unit('morpheme', parent=words[idx])
                    idx +=1
                    mb_seg = re.split(r'(?=-|\=)', mb)
                    for morph in mb_seg:
                        morph = morph.strip()
                        morphemes.append(mid)
                        self.ensure_feature('morpheme','SFM','form','str')
                        self.set_feature(mid, 'SFM', 'form', morph)
            elif l.startswith('\\gl'):
                m_glosses = l.replace('\\gl','').strip().split('&')
                for mg in m_glosses:
                    mg_seg = re.split(r'(?=-|\=)', mg)
                    for gloss in mg_seg:
                        gloss = gloss.strip()
                        
                        self.ensure_feature('morpheme', 'SFM', 'gls', 'str')
                        self.set_feature(morphemes[0], 'SFM', 'gls', gloss)
                        morphemes.pop(0)
            elif l.startswith('\\ft'):
                translation = l.replace('\\ft', '').strip()
                self.ensure_feature('sentence', 'SFM', 'translation', 'str')
                self.set_feature(sent_id, 'SFM', 'translation', translation)
        self.commit()
