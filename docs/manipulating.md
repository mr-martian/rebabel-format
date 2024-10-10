# Manipulating Data

There are two main ways of controling import and export processes: mapping and merging. Mapping renames types and features for the duration of the import or export process while merging specifies that an importer should add features to existing units rather than creating new ones.

## Mapping

Due to the difficulty (indeed, probably impossibility) of creating a naming system that covers all use-cases, reBabel expects each format converter to operate in a separate namespace. Thus, the FLExText importer will create features beginning with `FlexText:` while the CoNLL-U importer creates ones starting with `UD:`. Likewise, the exporters for those formats check for features with the same prefixes and so a conversion from one filetype to another without any parameters will output a mostly empty document.

The solution to this is mappings. Mappings define temporary renamings of types or features during import and export. There are three types of mappings: type, feature, and restricted feature.

### Type Mapptings

```toml
[[mappings]]
in_type = 'sentence'
out_type = 'phrase'
```

A type mapping establishes an equivalence between two different type names. In the mapping above, we declare that what is listed in the database as `sentence` should be treated by the format converter as if it said `phrase`. So, since the CoNLL-U converters operate on `sentence` nodes and the FLExText converters operate no `phrase` nodes, we could use this mapping to convert from CoNLL-U to FLExText. We could put this in either the import step or the export step (but not both) depending on whether we wanted the contents of the database to follow FLExText conventions (import) or CoNLL-U conventions (export).

### Feature Mappings

```toml
[[mappings]]
in_feature = 'UD:lemma'
out_feature = 'FlexText:en:lem'
```

Similarly, a feature mapping establishes an equivalence for a particular feature name. Here we're mapping between the CoNLL-U `UD:lemma` and the FLExText `FlexText:en:lem`.

### Restricted Feature Mappings

```toml
[[mappings]]
in_feature = 'UD:sent_id'
in_type = 'sentence'
out_feature = 'FlexText:en:sid'
```

Finally, a restricted feature mapping does the same thing as a feature mapping, but only applies to features on a particular unit type. The type restriction can be specified by either `in_type` or `out_type`.

## Merging

When two files contain the same data, we can combine them if there is some ID field that both importers would create, such as `meta:index`.

```toml
[import.merge_on]
'interlinear-text' = 'meta:index'
paragraph = 'meta:index'
phrase = 'meta:index'
word = 'meta:index'
morph = 'meta:index'
```

This states that nodes of the 5 listed types (which are the 5 main types created by the FLExText importer) should be merged if they share the same value of `meta:index` and (if applicable) the same parent. Thus if we are merging the data in two FLExText files, the `interlinear-text` nodes which have the same indecies will be treated as the same unit, and within each text, any `paragraph` nodes which have the same index will be treated as the same and so on.
