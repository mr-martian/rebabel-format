#!/usr/bin/env python3

from .reader import XMLReader
from collections import Counter

class EAFReader(XMLReader):
    identifier = 'eaf'
    short_name = 'ELAN'
    long_name = 'ELAN annotation file'

    def read_file(self, root):
        self.db.committing = False
        self.db.current_time = self.db.now()
        self.units = {} # ELAN ID -> (DB ID, type)
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
        self.commit_features()
        self.db.committing = True
        self.db.current_time = None
        self.db.commit()

    def annotation_text(self, node):
        for ch in node:
            if ch.tag == 'ANNOTATION_VALUE':
                return ch.text
        return None

    def process_tier(self, tier_name, tier, parent_relation):
        units = True
        index = False
        self.time_ranges[tier_name] = {}
        parent_tier = tier.attrib.get('PARENT_REF')
        if parent_relation == 'Symbolic_Association':
            units = False
        if parent_relation in [None, 'Time_Subdivision', 'Included_In']:
            self.ensure_feature(tier_name, 'alignment', 'starttime', 'int')
            self.ensure_feature(tier_name, 'alignment', 'endtime', 'int')
        elif parent_relation == 'Symbolic_Subdivision':
            self.ensure_feature(tier_name, 'alignment', 'index', 'int')
            index = True
        if units:
            self.db.ensure_type(tier_name)
        index_val = Counter()
        for ann_container in tier:
            if ann_container.tag != 'ANNOTATION':
                continue
            for ann in ann_container:
                if ann.tag == 'REF_ANNOTATION':
                    t = self.annotation_text(ann)
                    p = ann.attrib.get('ANNOTATION_REF')
                    xmlid = ann.attrib.get('ANNOTATION_ID')
                    uid, typ = self.units[p]
                    if t is not None:
                        if units:
                            i = self.create_unit(tier_name, uid)
                            if index:
                                index_val[uid] += 1
                                self.prepare_feature(i, 'alignment', 'index',
                                                     index_val[uid])
                            uid, typ = i, tier_name
                        self.ensure_feature(typ, 'ELAN', tier_name, 'str')
                        self.prepare_feature(uid, 'ELAN', tier_name, t)
                    self.units[xmlid] = (uid, typ)
                elif ann.tag == 'ALIGNABLE_ANNOTATION':
                    t = self.annotation_text(ann)
                    i = ann.attrib.get('ANNOTATION_ID')
                    s = self.times.get(ann.attrib.get('TIME_SLOT_REF1'), -1)
                    e = self.times.get(ann.attrib.get('TIME_SLOT_REF2'), -1)
                    self.time_ranges[tier_name][(s, e)] = i
                    parent = None
                    if parent_relation in ['Time_Subdivision', 'Included_In']:
                        dct = self.time_ranges.get(parent_tier, {})
                        for (start, end), ann_id in dct.items():
                            if start <= s and e <= end:
                                parent = self.units[ann_id][0]
                                break
                    uid = self.create_unit(tier_name, parent)
                    self.prepare_feature(uid, 'alignment', 'starttime', s)
                    self.prepare_feature(uid, 'alignment', 'endtime', e)
                    self.units[i] = (uid, tier_name)
                    if t is not None:
                        self.ensure_feature(tier_name, 'ELAN', tier_name, 'str')
                        self.prepare_feature(uid, 'ELAN', tier_name, t)
