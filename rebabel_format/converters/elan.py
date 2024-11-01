#!/usr/bin/env python3

from rebabel_format.reader import XMLReader
from rebabel_format.writer import Writer
from rebabel_format.parameters import Parameter
from rebabel_format import utils

from collections import Counter
from dataclasses import dataclass
from xml.etree import ElementTree as ET

@dataclass
class ELANTier:
    name: str
    parent: str = None
    relation: str = None
    aligned: bool = False
    node: ET.Element = None

def get_tier_structure(root):
    ling_type = {}
    for type_node in root.findall('LINGUISTIC_TYPE'):
        attr = type_node.attrib
        ling_type[attr.get('LINGUISTIC_TYPE_ID')] = (
            attr.get('CONSTRAINTS'), (attr.get('TIME_ALIGNABLE') == 'true'))
    tiers = {}
    for tier_node in root.findall('TIER'):
        attr = tier_node.attrib
        name = attr.get('TIER_ID')
        obj = ELANTier(name)
        obj.parent = attr.get('PARENT_REF')
        obj.relation, obj.aligned = ling_type.get(
            attr.get('LINGUISTIC_TYPE_REF'))
        obj.node = tier_node
        tiers[name] = obj
    order = []
    done = {None}
    todo = sorted(tiers.keys())
    while todo:
        next_todo = []
        for t in todo:
            if tiers[t].parent in done:
                order.append(t)
                done.add(t)
            else:
                next_todo.append(t)
        todo = next_todo
    return tiers, order

class EAFReader(XMLReader):
    '''
    Any tier which does not a Symbolic Association tier will be added as a unit
    of the same name. Every tier will be imported as a string feature in the
    tier `ELAN`.

    If a tier is time-aligned, then the units imported from it will
    have integer features `alignment:startime` and `alignment:endtime`,
    which will be the start and end times in milliseconds.

    If a tier is not time-aligned (i.e. if it is a Symbolic Subdivision),
    then the units imported from it will have the integer feature
    `alignment:index` which starts at `1` for a given parent element.
    '''
    identifier = 'eaf'
    short_name = 'ELAN'
    long_name = 'ELAN annotation file'
    format_specification = 'https://www.mpi.nl/tools/elan/EAF_Annotation_Format_3.0_and_ELAN.pdf'

    def read_file(self, root):
        self.names = {}
        self.times = {}
        self.time_ranges = {} # tier ID -> (start, end) -> annotation id
        for tm in root.findall('TIME_SLOT'):
            v = int(tm.attrib.get('TIME_VALUE', '-1'))
            self.times[tm.attrib.get('TIME_SLOT_ID')] = v
        tiers, order = get_tier_structure(root)
        for t in order:
            self.process_tier(t, tiers[t].node, tiers[t].relation)
        self.finish_block()

    def annotation_text(self, node):
        for ch in node:
            if ch.tag == 'ANNOTATION_VALUE':
                return ch.text
        return None

    def process_tier(self, tier_name, tier, parent_relation):
        units = (parent_relation != 'Symbolic_Association')
        index = (parent_relation == 'Symbolic_Subdivision')
        self.time_ranges[tier_name] = {}
        parent_tier = tier.attrib.get('PARENT_REF')
        index_val = Counter()
        for ann_container in tier:
            if ann_container.tag != 'ANNOTATION':
                continue
            for ann in ann_container:
                if ann.tag == 'REF_ANNOTATION':
                    t = self.annotation_text(ann)
                    p = ann.attrib.get('ANNOTATION_REF')
                    xmlid = ann.attrib.get('ANNOTATION_ID')
                    name = self.names[p]
                    if t is not None:
                        if units:
                            self.set_type(xmlid, tier_name)
                            self.set_parent(xmlid, name)
                            if index:
                                index_val[name] += 1
                                self.set_feature(
                                    xmlid, 'alignment:index', 'int',
                                    index_val[name])
                            name = xmlid
                        self.set_feature(name, 'ELAN:'+tier_name, 'str', t)
                    self.names[xmlid] = name
                elif ann.tag == 'ALIGNABLE_ANNOTATION':
                    t = self.annotation_text(ann)
                    i = ann.attrib.get('ANNOTATION_ID')
                    s = self.times.get(ann.attrib.get('TIME_SLOT_REF1'), -1)
                    e = self.times.get(ann.attrib.get('TIME_SLOT_REF2'), -1)
                    self.time_ranges[tier_name][(s, e)] = i
                    if parent_relation in ['Time_Subdivision', 'Included_In']:
                        dct = self.time_ranges.get(parent_tier, {})
                        for (start, end), ann_id in dct.items():
                            if start <= s and e <= end:
                                self.set_parent(i, self.names[ann_id])
                                break
                    self.names[i] = i
                    self.set_type(i, tier_name)
                    self.set_feature(i, 'alignment:starttime', 'int', s)
                    self.set_feature(i, 'alignment:endtime', 'int', e)
                    if t is not None:
                        self.set_feature(i, 'ELAN:'+tier_name, 'str', t)

class EAFWriter(Writer):
    identifier = 'eaf'

    query = True

    template_file = Parameter(type=str, required=True)
    seconds = Parameter(type=bool, default=False, help='interpret time offsets as seconds rather than miliseconds')

    def pre_query(self):
        self.tree = ET.parse(self.template_file).getroot()
        self.tiers, order = get_tier_structure(self.tree)
        self.query = {}
        self.query_order = []
        for tier_name in order:
            tier = self.tiers[tier_name]
            while len(tier.node) > 0:
                tier.node.remove(tier.node[0])
            if not tier.parent or tier.relation != 'Symbolic_Association':
                self.query_order.append(tier_name)
                self.query[tier.name] = {
                    'type': tier.name,
                    'parent': tier.parent,
                    'order': 'alignment:starttime' if tier.aligned else 'alignment:index',
                }

    def write(self, fout):
        feat_names = {}
        for name in self.query_order:
            main = ['ELAN:'+name]
            if self.tiers[name].aligned:
                main += ['alignment:starttime', 'alignment:endtime']
            feats = []
            for tier in self.tiers.values():
                if tier.parent == name and tier.relation == 'Symbolic_Association':
                    feats.append('ELAN:'+tier.name)
            self.table.add_features(name, main+feats, error=False)
            feat_names[name] = feats
        parent_annotations = {}
        times = {0}
        ann_count = 0
        for nodes, result_feats in self.table.results():
            for name in self.query_order:
                for uid in utils.as_list(nodes.get(name)):
                    if uid in parent_annotations or uid is None:
                        continue
                    feat_values = result_feats[uid]
                    feats = feat_names[name]
                    ann_count += 1
                    ann_id = f'ann{ann_count}'
                    parent_annotations[uid] = ann_id

                    ann1 = ET.SubElement(self.tiers[name].node, 'ANNOTATION')
                    if self.tiers[name].aligned:
                        start = feat_values.get('alignment:starttime')
                        end = feat_values.get('alignment:endtime')
                        if start is None:
                            if not end:
                                start = max(times)
                                end = start + 1
                            else:
                                start = end - 1
                        elif end is None:
                            end = start + 1
                        times.add(start)
                        times.add(end)
                        ann2 = ET.SubElement(ann1, 'ALIGNABLE_ANNOTATION',
                                             ANNOTATION_ID=ann_id,
                                             TIME_SLOT_REF1=f'ts{start}',
                                             TIME_SLOT_REF2=f'ts{end}')
                    else:
                        ann2 = ET.SubElement(ann1, 'REF_ANNOTATION',
                                             ANNOTATION_ID=ann_id)
                        pid = nodes.get(self.tiers[name].parent)
                        if pid and pid in parent_annotations:
                            pref = parent_annotations[pid]
                            ann2.attrib['ANNOTATION_REF'] = pref
                            if len(self.tiers[name].node) > 1:
                                prev = self.tiers[name].node[-2]
                                if prev[0].attrib['ANNOTATION_REF'] == pref:
                                    ann2.attrib['PREVIOUS_ANNOTATION'] = prev[0].attrib['ANNOTATION_ID']
                    ann3 = ET.SubElement(ann2, 'ANNOTATION_VALUE')
                    content = feat_values.get('ELAN:'+name)
                    if content is not None:
                        ann3.text = str(content)

                    for feat, value in feat_values.items():
                        if value is None:
                            continue
                        feat_name = feat[5:]
                        ann1 = ET.SubElement(self.tiers[feat_name].node,
                                             'ANNOTATION')
                        ann_count += 1
                        ann2 = ET.SubElement(ann1, 'REF_ANNOTATION',
                                             ANNOTATION_ID=f'ann{ann_count}',
                                             ANNOTATION_REF=ann_id)
                        ann3 = ET.SubElement(ann2, 'ANNOTATION_VALUE')
                        ann3.text = str(value)

        time_node = self.tree.find('TIME_ORDER')
        time_node.clear()
        time_node.tail = '\n'
        for tm in sorted(times):
            val = str(tm*1000) if self.seconds else str(tm)
            ET.SubElement(time_node, 'TIME_SLOT', TIME_SLOT_ID=f'ts{tm}',
                          TIME_VALUE=val)
        tree = ET.ElementTree(self.tree)
        tree.write(fout, encoding='unicode', xml_declaration=True)
