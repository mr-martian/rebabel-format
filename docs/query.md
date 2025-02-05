# Querying Data

There are two interfaces for searching a dataset: a Python API and a custom query language.
The two are equivalent in power, though users who need to run many slight variations of the same query may find the Python API more amenable to scripting.

## Python API

The following query searches for nouns with suffixes.

```python
# import the relevant modules
from rebabel_format.db import RBBLFile
from rebabel_format.query import Query

# create the main objects
db = RBBLFile('flextext_data.db')
q = Query(db)

# specify the units we're looking for
w = q.unit('word', 'w')
m1 = q.unit('morph', 'm1')
m2 = q.unit('morph', 'm2')

# condition the units
q.add(m1.parent(w))
q.add(m2.parent(w))
q.add(m1['meta:index'] + 1 == m2['meta:index'])
q.add(m1['FlexText:en:msa'].startswith('n'))

# get the results
for result in q.search():
    print(result)
    # {'w': 1234, 'm1': 1237, 'm2': 1238}
    # ...
```

This example loads a database with FLExText feature names.
It then specifies that it is searching for three units (the `q.unit` calls).
The first unit will be listed in the output as `w` and will be a `word`.
The the other two will be `morph`s named `m1` and `m2`.
(The Python variables don't have to match the names, but it makes it easier to read.)

After listing the units, it adds conditions (`q.add`) on the relationships between the units, such as that both morphemes must be contained in the word (`m1.parent(w)`).
It also specifies that the second morpheme must come immediately after the first one (the condition on `meta:index`).
Finally, it states that the `FlexText:en:msa` feature (part-of-speech tag) of the first morpheme must start with `n`.

Iterating over the results of this query produces a sequence of dictionaries which map the names of the units to their database IDs.

## Query Language

The following is equivalent to the query in the previous section:

```
unit w word
unit m1 morph
unit m2 morph
m1 parent w
m2 parent w
m1.meta:index + 1 = m2.meta:index
m1.FlexText:en:msa startswith "n"
```
