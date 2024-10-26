from rebabel_format.parameters import Parameter
from rebabel_format.reader import Reader
from rebabel_format.writer import Writer
import csv

class CSVReader(Reader):
    '''
    Units imported:

    - entry

    Each row of the spreadsheet will be imported as a unit with the features
    named by the column labels in the `csv:` tier.
    '''

    identifier = 'csv'
    dialect = Parameter(choices=csv.list_dialects(), default='excel')
    delimiter = Parameter(type=str, default=',')
    quotechar = Parameter(type=str, default='"')

    def open_file(self, pth):
        return open(pth, newline='')

    def read_file(self, fin):
        reader = csv.DictReader(fin, dialect=self.dialect, delimiter=self.delimiter,
                                quotechar=self.quotechar)
        for num, row in enumerate(reader):
            self.set_type(num, 'entry')
            for key, value in row.items():
                if key is None or value is None:
                    continue
                self.set_feature(num, 'csv:'+key, 'str', value)
        self.finish_block()

class CSVWriter(Writer):
    identifier = 'csv'
    dialect = Parameter(choices=csv.list_dialects(), default='excel')
    delimiter = Parameter(type=str, default=',')
    quotechar = Parameter(type=str, default='"')
    includeid = Parameter(type=bool, default=False)
    query = {'entry': {'type': 'entry'}}

    def write(self, fout):
        self.table.add_tier('entry', 'csv')
        tier_names = sorted(n[4:] for n in self.table.feature_names.values()
                            if n.startswith('csv:'))
        prefix = []
        if self.includeid:
            prefix = ['ID']
        writer = csv.DictWriter(fout, prefix+tier_names, extrasaction='ignore')
        writer.writeheader()

        for nodes, features in self.table.results():
            i = nodes['entry']
            dct = {t:features[i].get('csv:'+t) for t in tier_names}
            if self.includeid:
                dct['ID'] = i
            writer.writerow(dct)
