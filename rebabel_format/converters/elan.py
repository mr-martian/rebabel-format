#!/usr/bin/env python3

from rebabel_format.reader import XMLReader
from collections import Counter

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
        tiers = {} # tier ID -> XML node
        tier_wait = {} # tier ID -> (tier ID)
        ling_type = {} # type ID -> constraint name
        tier_type = {}
        for tm in root.findall('TIME_SLOT'):
            v = int(tm.attrib.get('TIME_VALUE', '-1'))
            self.times[tm.attrib.get('TIME_SLOT_ID')] = v
        for type_node in root.findall('LINGUISTIC_TYPE'):
            i = type_node.attrib.get('LINGUISTIC_TYPE_ID')
            ling_type[i] = type_node.attrib.get('CONSTRAINTS')
        for tier_node in root.findall('TIER'):
            i = tier_node.attrib.get('TIER_ID')
            tiers[i] = tier_node
            t = tier_node.attrib.get('LINGUISTIC_TYPE_REF')
            tier_wait[i] = tier_node.attrib.get('PARENT_REF')
            tier_type[i] = ling_type.get(t)
        todo = sorted(tiers.keys())
        done = set()
        done.add(None)
        while len(todo) > 0:
            next_todo = []
            for t in todo:
                if tier_wait[t] in done:
                    self.process_tier(t, tiers[t], tier_type[t])
                    done.add(t)
                else:
                    next_todo.append(t)
            todo = next_todo
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
                                    xmlid, 'alignment', 'index', 'int',
                                    index_val[name])
                            name = xmlid
                        self.set_feature(name, 'ELAN', tier_name, 'str', t)
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
                    self.set_feature(i, 'alignment', 'starttime', 'int', s)
                    self.set_feature(i, 'alignment', 'endtime', 'int', e)
                    if t is not None:
                        self.set_feature(i, 'ELAN', tier_name, 'str', t)
