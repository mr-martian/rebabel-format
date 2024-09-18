# Readers

Readers import data into a reBabel database.

```python
class SomeReader(Reader):
    identifier = 'something'

    version = Parameter(type=int, default=3, help='schema version to read')

    def read_file(self, fin):
        for line_number, line in enumerate(fin):
            self.set_type(line_number, 'line')
            self.set_feature(line_number, 'something', 'line', 'str',
                             line.strip())
        self.finish_block()
```

Readers should have an attribute named `identifier` in order to be invokable.

Any [parameters](parameters.md) that need to be specified should also be specified as class attributes.

The action of a reader is broken across the methods `open_file(path)`, `read_file(file)`, and `close_file(file)`. By default `open_file` opens a file in text mode and `close_file` calls `.close()` on it, but these can be overridden (and see below regarding subclasses for common cases).

Within `read_file`, units are created when information about them is specified, which is done with the following methods:

- `set_type(name, type)`: specify the unit type of `name`; if the type of a unit is not specified, a `ReaderError` will be raised
- `set_parent(child_name, parent_name)`: set the primary parent of a given unit
- `add_relation(child_name, parent_name)`: set a non-primary parent of a given unit
- `set_feature(name, tier, feature, type, value)`: set `tier:feature` to `value` for unit `name`, creating the feature with type `type`, if necessary
- `finish_block(keep_uids=False)`: indicates that a segment of data is complete and should be committed to the database
  - by default, the list of names accumulated by the other methods will be cleared; this can be prevented by setting `keep_uids=True`, which is useful for cases where the input has globally unique IDs, is very large, and has relations spanning the file

Unit names are purely internal to the `Reader` instance and can be of any hashable type (`int`, `str`, `tuple`, etc). They will be converted to database IDs when `finish_block` is called.

## `XMLReader`

This subclass parses the input file using [`ElementTree`](https://docs.python.org/3/library/xml.etree.elementtree.html) and passes an `Element` object to `read_file`.

## `JSONReader`

This subclass parses the input file as JSON and passes a dictionary to `read_file`.

## `LineReader`

This subclass is specialized for text files where linebreaks are meaningful. It is roughly equivalent to the following:

```python
for line in file:
    if self.is_boundary(line):
        self.end()
        self.reset()
    self.process_line(line)
```

- `is_boundary(line)`: should return `True` if `line` is the end of a group of lines or the beginning of a new one; by default it checks if the line is blank
- `process_line(line)`: perform any processing needed on the text of the line
- `end()`: hook to operate at the end of a block; calls `finish_block()`
- `reset()`: set up any needed variables for a new block
