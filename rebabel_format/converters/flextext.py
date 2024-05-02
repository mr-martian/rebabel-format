#!/usr/bin/env python3

from .reader import XMLReader
from .writer import Writer
from ..config import get_single_param

class FlextextReader(XMLReader):
    identifier = 'flextext'
    short_name = 'FlexText'
    long_name = 'SIL Fieldworks Language Explorer XML glossed text'

    def read_file(self, fin):
        self.db.committing = False
        self.db.current_time = self.db.now()
        self.iter_nodes(fin)
        self.db.committing = True
        self.db.current_time = None
        self.db.commit()

    def iter_nodes(self, node, parent=None, idx=0):
        known = ['interlinear-text', 'paragraph', 'phrase', 'word', 'morph']
        if node.tag in known:
            self.ensure_feature(node.tag, 'meta', 'index', 'int')
            children = []
            feats = [('meta', 'index', idx)]
            i = 1
            for ch in node:
                if ch.tag == 'item':
                    tier = 'FlexText/'+ch.attrib.get('lang', 'None')
                    feat = ch.attrib.get('type', 'None')
                    val = ch.text or ''
                    self.ensure_feature(node.tag, tier, feat, 'str')
                    feats.append((tier, feat, val))
                else:
                    children.append((ch, i))
                    i += 1
            uid = self.create_unit_with_features(node.tag, feats, parent=parent)
            for ch, i in children:
                self.iter_nodes(ch, parent=uid, idx=i)
        else:
            for ch in node:
                self.iter_nodes(ch, parent=parent, idx=idx)
                if node.tag == 'document':
                    idx += 1

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
