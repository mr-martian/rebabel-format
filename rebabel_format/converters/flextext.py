#!/usr/bin/env python3

from rebabel_format.reader import XMLReader
from rebabel_format.writer import Writer
from rebabel_format.parameters import Parameter

ANALYSIS_STATUSES = [
    ('humanApproved', 4),
    ('guessByHumanApproved', 3),
    ('guessByStatisticalAnalysis', 2),
    ('guess', 1),
]

# TODO: should we record <document version= exportSource= exportTarget=>?
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
    format_specification = 'https://github.com/sillsdev/FieldWorks/blob/release/9.1/DistFiles/Language%20Explorer/Export%20Templates/Interlinear/FlexInterlinear.xsd'

    def read_file(self, fin):
        self.iter_nodes(fin)
        self.finish_block()

    def iter_nodes(self, node, parent=None, idx=0):
        known = ['interlinear-text', 'paragraph', 'phrase', 'word', 'morph',
                 'scrMilestone', 'language', 'media']
        if node.tag in known:
            name = node.attrib.get('guid', (parent or '') + ' ' + str(idx))
            self.set_type(name, node.tag)
            if parent:
                self.set_parent(name, parent)
            self.set_feature(name, 'meta', 'index', 'int', idx)
            for feat, val in node.attrib.items():
                typ = 'str'
                if feat in ['chapter', 'verse']:
                    typ = 'int'
                    val = int(val)
                self.set_feature(name, 'FlexText', feat, typ, val)
            chidx = 0
            for ch in node:
                if ch.tag == 'item':
                    tier = 'FlexText/'+ch.attrib.get('lang', 'None')
                    feat = ch.attrib.get('type', 'None')
                    val = ch.text or ''
                    confidence = None
                    if 'analysisStatus' in ch.attrib:
                        confidence = dict(ANANLYSIS_STATUSES).get(
                            ch.attrib['analysisStatus'], 0)
                    self.set_feature(name, tier, feat, 'str', val,
                                     confidence=confidence)
                else:
                    chidx += 1
                    self.iter_nodes(ch, parent=name, idx=chidx)
        else:
            if parent is not None:
                for feat, val in node.attrib.items():
                    self.set_feature(parent, 'FlexText', feat, 'str', val)
            for i, ch in enumerate(node, 1):
                self.iter_nodes(ch, parent=parent, idx=i)

# TODO: export scrMilestone, language, media
# TODO: confidence → analysisStatus
# TODO: optionally warn on values that aren't in the list defined by the XSD
class FlextextWriter(Writer):
    identifier = 'flextext'

    root = Parameter(type=str, default='interlinear-text', required=False)
    skip = Parameter(type=list, required=False)

    query_order = ['interlinear-text', 'paragraph', 'phrase', 'word', 'morph']
    query = {
        'interlinear-text': {
            'type': 'interlinear-text',
            'order': 'meta:index',
        },
        'paragraph': {
            'type': 'paragraph',
            'parent': 'interlinear-text',
            'order': 'meta:index',
        },
        'phrase': {
            'type': 'phrase',
            'parent': 'paragraph',
            'order': 'meta:index',
        },
        'word': {
            'type': 'word',
            'parent': 'phrase',
            'order': 'meta:index',
        },
        'morph': {
            'type': 'morph',
            'parent': 'word',
            'order': 'meta:index',
        },
    }

    def rem_layer(self, layer):
        if layer not in self.query:
            return
        next_layer = None
        for i in range(self.query_order.index(layer)+1, 5):
            if self.query_order[i] in self.query:
                next_layer = self.query_order[i]
                break
        parent = self.query[layer].get('parent')
        del self.query[layer]
        if next_layer:
            if parent:
                self.query[next_layer]['parent'] = parent
            else:
                del self.query[next_layer]['parent']

    def pre_query(self):
        if self.root is not None:
            for i, layer in enumerate(self.query_order):
                if layer == self.root:
                    break
                else:
                    self.rem_layer(layer)
            else:
                raise ValueError(f"Unknown value for 'root' in flextext export '{top_layer}'.")
        for layer in (self.skip or []):
            self.rem_layer(layer)

    def indent(self, node, depth):
        rem = []
        for c in node:
            self.indent(c, depth+1)
            if len(c) == 0 and not c.attrib:
                rem.append(c)
        for r in rem:
            node.remove(r)
        if len(node) > 0:
            node[-1].tail = node[-1].tail[:-2]
            node.text = '\n  ' + '  '*depth
        node.tail = '\n' + '  '*depth

    def write(self, fout):
        import xml.etree.ElementTree as ET
        group_names = {
            'interlinear-text': 'paragraphs',
            'paragraph': 'phrases',
            'phrase': 'words',
            'word': 'morphemes',
        }
        tree = ET.Element('document', version='2')
        feats = {}
        for layer in self.query_order:
            if layer in self.query:
                feats[layer] = sorted(
                    self.table.add_tier(layer, 'FlexText', prefix=True).items())
        uid2elem = {}
        for id_dict, feat_dict in self.table.results():
            elem = None
            parent = tree
            for depth, layer in enumerate(self.query_order):
                lid = id_dict.get(layer, layer)
                if lid in uid2elem:
                    elem, parent = uid2elem[lid]
                    continue
                elem = ET.SubElement(parent, layer)
                if layer == 'morph':
                    parent = None
                else:
                    parent = ET.SubElement(elem, group_names[layer])
                uid2elem[lid] = (elem, parent)
                unit_feats = feat_dict.get(lid, {})
                for (tier, feat), fid in feats.get(layer, []):
                    val = unit_feats.get(fid)
                    if val is None:
                        continue
                    if tier.startswith('FlexText/'):
                        i = ET.SubElement(elem, 'item', lang=tier[9:], type=feat)
                        i.text = str(val)
        self.indent(tree, 0)
        ET.ElementTree(element=tree).write(
            fout, encoding='unicode', xml_declaration=True)
