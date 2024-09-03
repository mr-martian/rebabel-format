from rebabel_format.reader import Reader
import os

class TextFabricReader(Reader):
    '''
    The input file for this reader must be a directory containing files with
    the extension `.tf`. The unit type is taken from the file `otype.tf`
    while all other node features are imported as features with the same
    name in the `textfabric` tier. The value type is either string or integer
    as specified in the feature file.

    Each unit also has an integer feature named `textfabric-meta:id`, which
    contains the numeric ID of the unit in Text-Fabric.

    Edges and edge features are imported as units of type `[name]-tf-link`,
    which have reference features `textfabric-meta:parent` and
    `textfabric-meta:child`, and a string or integer feature
    `textfabric-meta:value`, if applicable.
    '''

    identifier = 'textfabric'
    format_specification = 'https://annotation.github.io/text-fabric/tf/about/fileformats.html'

    def open_file(self, pth):
        if not os.path.isdir(pth):
            self.error(f"Path '{pth}' is not a directory.")
        if not os.path.isfile(os.path.join(pth, 'otype.tf')):
            self.error(f"Type file not found in '{pth}'.")
        return pth

    def close_file(self, pth):
        pass

    def read_file(self, pth):
        import glob
        fnames = sorted(glob.glob(os.path.join(pth, '*.tf')))
        if not fnames:
            self.error(f"No .tf files found in {pth}.")
        otype_path = os.path.join(pth, 'otype.tf')
        with open(otype_path) as fin:
            self.filename = otype_path
            self.read_tf_file('otype', fin)
            self.finish_block(keep_uids=True)
            self.info(f"Done reading '{otype_path}'.")
        fnames = [fn for fn in fnames if not fn.endswith('otype.tf')]
        for index, name in enumerate(fnames, 1):
            feature_name = os.path.splitext(os.path.basename(name))[0]
            if feature_name == 'otype':
                continue
            with open(name) as fin:
                self.filename = name
                self.read_tf_file(feature_name, fin)
            self.finish_block(keep_uids=True)
            self.info(f"Done reading '{name}' ({index} / {len(fnames)}).")

    def parse_node_spec(self, spec):
        ret = set()
        for piece in spec.split(','):
            if piece.isdigit():
                ret.add(int(piece))
            elif piece.count('-') == 1:
                a, b = piece.split('-')
                if not a.isdigit() or not b.isdigit():
                    self.error(f"Could not parse specifier '{piece}'.")
                a = int(a)
                b = int(b)
                ret.update(range(min(a, b), max(a, b)+1))
            else:
                self.error(f"Could not parse specifier '{piece}'.")
        return ret

    def maybe_intify(self, value, is_str):
        if is_str:
            return value
        elif not value:
            return None
        try:
            return int(value)
        except:
            pass
        self.error("Invalid integer value '{value}'.")

    def read_tf_file(self, feature_name, fin):
        in_header = True
        is_node = True
        is_str = True
        edge_values = False
        last_node = 0

        for linenumber, raw_line in enumerate(fin, 1):
            self.location = f'line {linenumber}'
            if not raw_line:
                continue
            line = raw_line.rstrip('\n')
            if in_header:
                if not line:
                    in_header = False
                    continue
                elif line[0] == '@':
                    if line == '@config':
                        return
                    elif line == '@edge':
                        is_node = False
                    elif line == '@edgeValues':
                        edge_values = True
                    elif line == '@valueType=int':
                        is_str = False
                    continue
                else:
                    # no blank line, just continue to body
                    pass
            pieces = line.split('\t')
            if is_node:
                if len(pieces) == 1:
                    nodes = {last_node+1}
                    value = self.maybe_intify(pieces[0], is_str)
                elif len(pieces) == 2:
                    nodes = self.parse_node_spec(pieces[0])
                    value = self.maybe_intify(pieces[1], is_str)
                else:
                    self.error(f'Too many columns (found {len(pieces)}).')
                last_node = max(nodes)

                if feature_name == 'otype':
                    for node in nodes:
                        self.set_type(node, value)
                        self.set_feature(node, 'textfabric-meta', 'id', 'int',
                                         node)
                elif value:
                    for node in nodes:
                        self.set_feature(node, 'textfabric', feature_name,
                                         'str' if is_str else 'int', value)
            else:
                if len(pieces) == 1:
                    nfrom = {last_node+1}
                    nto = self.parse_node_spec(pieces[0])
                    value = None
                elif len(pieces) == 2:
                    if edge_values:
                        nfrom = {last_node+1}
                        nto = self.parse_node_spec(pieces[0])
                        value = self.maybe_intify(pieces[1], is_str)
                    else:
                        nfrom = self.parse_node_spec(pieces[0])
                        nto = self.parse_node_spec(pieces[1])
                        value = None
                elif len(pieces) == 3 and edge_values:
                    nfrom = self.parse_node_spec(pieces[0])
                    nto = self.parse_node_spec(pieces[1])
                    value = self.maybe_intify(pieces[2], is_str)
                else:
                    self.error(f'Too many columns (found {len(pieces)}).')
                last_node = max(nfrom|nto)

                for f in nfrom:
                    for t in nto:
                        self.set_type((t, f), feature_name + '-tf-link')
                        self.set_feature((t, f), 'textfabric-meta', 'parent',
                                         'ref', f)
                        self.set_feature((t, f), 'textfabric-meta', 'child',
                                         'ref', t)
                        if value:
                            self.set_feature((t, f), 'textfabric-meta', 'value',
                                             'str' if is_str else 'int', value)
