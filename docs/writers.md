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
        s_feat = self.table.add_features('S', ['something:text'])[0]

        w_feat_names = ['something:lemma', 'something:pos']
        w_feat_ids = self.table.add_features('W', w_feat_names)
        w_lemma = w_feat_ids[0]
        w_pos = w_feat_ids[1]

        current_sentence = None
        for units, features in self.table.results():
            if units['S'] != current_sentence:
                fout.write(str(features[units['S']].get(s_feat, '')) + '\n')
                current_sentence = units['S']
            fout.write(self.indent)
            fout.write(str(features[units['W']].get(w_lemma, '')))
            fout.write(' ')
            fout.write(str(features[units['W']].get(w_pos, '')))
            fout.write('\n')
```
