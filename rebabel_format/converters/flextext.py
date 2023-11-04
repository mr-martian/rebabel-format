#!/usr/bin/env python3

from .reader import Reader
import xml.etree.ElementTree as ET

class FlextextReader(Reader):
    identifier = 'flextext'
    short_name = 'FlexText'
    long_name = 'SIL Fieldworks Language Explorer XML glossed text'

    def open_file(self, pth):
        self.db.committing = False
        self.db.current_time = self.db.now()
        return ET.parse(pth).getroot()

    def close_file(self, fin):
        self.db.committing = True
        self.db.current_time = None
        self.db.commit()

    def read_file(self, fin):
        self.iter_nodes(fin)

    def iter_nodes(self, node, parent=None):
        known = ['interlinear-text', 'paragraph', 'phrase', 'word', 'morph']
        if node.tag in known:
            uid = self.create_unit(node.tag, parent=parent)
            for ch in node:
                if ch.tag == 'item':
                    tier = 'FlexText/'+ch.attrib.get('lang', 'None')
                    feat = ch.attrib.get('type', 'None')
                    val = ch.text or ''
                    self.ensure_feature(node.tag, tier, feat, 'str')
                    self.set_feature(uid, tier, feat, val)
                else:
                    self.iter_nodes(ch, parent=uid)
        else:
            for ch in node:
                self.iter_nodes(ch, parent=parent)
