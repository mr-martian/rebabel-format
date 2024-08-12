#!/usr/bin/env python3

from .reader import XMLReader
from .writer import Writer
from ..config import get_single_param

class FlextextReader(XMLReader):
    '''
    The imported unit types will be `interlinear-text`, `paragraph`, `phrase`,
    `word`, and `morph`, corresponding to the XML nodes of the same names.

    Each unit will have an integer feature `meta:index`, which counts from `1`
    for a given parent node.

    Any <item> nodes will be imported as string features. The tier will be
    `FlexText/[lang]`, the feature will be `[type]`, and the value will be
    the text content of the XML node.
    '''
    identifier = 'flextext'
    short_name = 'FlexText'
    long_name = 'SIL Fieldworks Language Explorer XML glossed text'

    def read_file(self, fin):
        self.iter_nodes(fin)
        self.finish_block()

    def iter_nodes(self, node, parent=None, idx=0):
        known = ['interlinear-text', 'paragraph', 'phrase', 'word', 'morph']
        if node.tag in known:
            name = node.attrib.get('guid', (parent or '') + ' ' + str(idx))
            self.set_type(name, node.tag)
            if parent:
                self.set_parent(name, parent)
            self.set_feature(name, 'meta', 'index', 'int', idx)
            chidx = 0
            for ch in node:
                if ch.tag == 'item':
                    tier = 'FlexText/'+ch.attrib.get('lang', 'None')
                    feat = ch.attrib.get('type', 'None')
                    val = ch.text or ''
                    self.set_feature(name, tier, feat, 'str', val)
                else:
                    chidx += 1
                    self.iter_nodes(ch, parent=name, idx=chidx)
        else:
            for i, ch in enumerate(node, 1):
                self.iter_nodes(ch, parent=parent, idx=i)

class FlextextWriter(Writer):
    identifier = 'flextext'

    def write(self, fout):
        import xml.etree.ElementTree as ET
        tiers = [x for x in self.all_tiers() if x.startswith('FlexText/')]
        tiers.append('meta')
        feat_dict = self.all_feats_from_tiers(tiers)
        feat_list = list(feat_dict.keys())
        meta_index = None
        for k, v in feat_dict.items():
            if v == ('meta', 'index', 'int'):
                meta_index = k

        def mkel(node, seq, parent):
            indent = 5 - len(seq)
            group, single = seq[0]
            units = list(self.iter_units(single, feat_list, parent))
            if meta_index is not None:
                units.sort(key=lambda x: x[1].get(meta_index, -1))
            if node is None:
                group_el = ET.Element(group)
            else:
                group_el = ET.SubElement(node, group)
            group_el.text = '\n  ' + '  '*indent*2
            ind = '\n    ' + '  '*indent*2
            for uid, feats in units:
                unit_el = ET.SubElement(group_el, single)
                unit_el.text = ind
                for k, v in sorted(feats.items()):
                    tier, feat, typ = feat_dict[k]
                    if not tier.startswith('FlexText/'):
                        continue
                    feat_el = ET.SubElement(unit_el, 'item')
                    feat_el.set('type', feat)
                    feat_el.set('lang', tier[9:])
                    feat_el.text = str(v)
                    feat_el.tail = ind
                if len(seq) > 1:
                    e = mkel(unit_el, seq[1:], uid)
                    e.tail = ind
                if indent > 0 and len(unit_el) > 0:
                    unit_el[-1].tail = unit_el[-1].tail[:-2]
                unit_el.tail = ind[:-2]
            if len(group_el) > 0:
                group_el[-1].tail = group_el[-1].tail[:-2]
            return group_el
        layers = [('document', 'interlinear-text'),
                  ('paragraphs', 'paragraph'),
                  ('phrases', 'phrase'),
                  ('words', 'word'),
                  ('morphemes', 'morph')]
        null_layers = []
        used_layers = layers[:]
        top_layer = get_single_param(self.conf, 'export', 'root')
        if top_layer is not None:
            for i in range(5):
                if layers[i][1] == top_layer:
                    null_layers = layers[:i]
                    used_layers = layers[i:]
                    break
            else:
                raise ValueError(f"Unknown value for 'root' in flextext export '{top_layer}'.")
        top = mkel(None, used_layers, None)
        for t1, t2 in reversed(null_layers):
            new_el = ET.Element(t1)
            sub = ET.SubElement(new_el, t2)
            sub.text = '\n'
            top.tail = '\n'
            sub.append(top)
            top = new_el
        top.set('version', '2')
        tree = ET.ElementTree(element=top)
        tree.write(fout, encoding='unicode', xml_declaration=True)
