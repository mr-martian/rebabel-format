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
        idx = 0
        for l in lines:
            if l.startswith('\\ref'):
                ref = l.split('.')[1]
                self.ensure_feature('sentence', 'SFM', 'reference', 'str')
                self.set_feature(sent_id, 'SFM', 'reference', ref)
            elif l.startswith('\\tx'):
                wds = l.replace('\\tx','').replace('\s\s+',' ').strip().split(' ')
                for word in wds:
                    word = word.strip()
                    wid = self.create_unit('word', parent=sent_id)
                    self.set_feature(wid, 'SFM', 'index', idx)
                    idx +=1
                    self.ensure_feature('word', 'SFM', 'form', 'str')
                    self.set_feature(wid, 'SFM', 'form', word)
            elif l.startswith('\\mb'):
                breaks = l.replace('\\mb','').strip().split('&')
                for mb in breaks:
                    #It would probably be better to make the word the parent?
                    mid = self.create_unit('morpheme', parent=sent_id)
                    mb_seg = re.split(r'(?=\=|-)', mb)
                    for morph in mb_seg:
                        #create a position index that references the word index and morpheme index
                        position = str(breaks.index(mb))+"."+str(mb_seg.index(morph))
                        morph = morph.strip()
                        self.ensure_feature('morpheme','SFM','index','str')
                        self.set_feature(mid, 'SFM', 'index', position)
                        self.ensure_feature('morpheme','SFM','form','str')
                        self.set_feature(mid, 'SFM', 'form', str(morph))
            elif l.startswith('\\gl'):
                m_glosses = l.replace('\\gl','').strip().split('&')
                for mg in m_glosses:
                    mgid = self.create_unit('morph_gloss', parent=sent_id)
                    mg_seg = re.split(r'(?=-|\=)', mg)
                    for gloss in mg_seg:
                        position = str(m_glosses.index(mg))+"."+str(mg_seg.index(gloss))
                        gloss = gloss.strip()
                        self.ensure_feature('morph_gloss', 'SFM', 'index', 'str')
                        self.set_feature(mgid, 'SFM', 'index', position)
                        self.ensure_feature('morph_gloss', 'SFM', 'form', 'str')
                        self.set_feature(mgid, 'SFM', 'form', str(gloss))
            elif l.startswith('\\ft'):
                translation = l.replace('\\ft', '').strip()
                self.ensure_feature('sentence', 'SFM', 'translation', 'str')
                self.set_feature(sent_id, 'SFM', 'translation', translation)
        self.commit()
