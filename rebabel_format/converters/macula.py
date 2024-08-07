from .reader import XMLReader

class MaculaNodeReader(XMLReader):
    identifier = 'macula-node'

    def read_file(self, root):
        node_ids = ['nodeId', '{http://www.w3.org/XML/1998/namespace}id']

        def get_node_id(node):
            for key in node_ids:
                if key in node.attrib:
                    return node.attrib[key]

        for sentence in root.iter('Sentence'):
            verse = sentence.attrib['verse']
            self.set_type(verse, 'sentence')
            for child in sentence.iter():
                nid = get_node_id(child)
                if nid is None:
                    continue
                if child.tag == 'm':
                    self.set_type(nid, 'morpheme')
                elif child.tag == 'Node':
                    self.set_type(nid, 'syntax-node')

                self.set_parent(nid, verse)

                for grandchild in child:
                    gid = get_node_id(grandchild)
                    if gid:
                        self.add_relation(gid, nid)

                if child.text and not child.text.isspace():
                    self.set_feature(nid, 'macula', 'form', 'str', child.text)

                for key, value in child.attrib.items():
                    if key in node_ids:
                        key = 'id'
                    self.set_feature(nid, 'macula', key, 'str', value)

        self.finish_block()
