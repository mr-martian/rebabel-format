# Writers

Writers define a query to extract relevant units and then write them to an output file.

```python
class SomeWriter(Writer):
    identifier = 'something'

    query = {
      'S': {'type': 'sentence'},
      'W': {'type': 'word', 'parent': 'S'},
    }
    query_order = ['S', 'W']

    indent = Parameter(type=str, default='\t')

    def write(self, fout):
        self.table.add_features('S', ['something:text'])

        w_feat_names = ['something:lemma', 'something:pos']
        self.table.add_features('W', w_feat_names)

        current_sentence = None
        for units, features in self.table.results():
            if units['S'] != current_sentence:
                fout.write(str(features[units['S']].get('something:text', '')) + '\n')
                current_sentence = units['S']
            fout.write(self.indent)
            fout.write(str(features[units['W']].get('something:lemma', '')))
            fout.write(' ')
            fout.write(str(features[units['W']].get('something:pos', '')))
            fout.write('\n')
```
