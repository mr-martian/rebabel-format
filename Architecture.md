# System Architecture

The primary product of this repository is a program named `rebabel-format`. This program accepts a process name and a [TOML](https://toml.io/en/) file of configuration options. It will eventually also be usable as a Python library.

## Databases

Processes operate on [SQLite](https://sqlite.org) databases of linguistic information. These databases contain three things:
- **Units** represent linguistic objects, such as sentences, words, or morphemes. They have a type, such as `"sentence"`.
- **Tiers** define what data can be associated with units. They have a feature name, such as `"Gender"`, a tier name, such as `"Morphology"` (so that tiers can be grouped), what unit type they apply to, and what type of value they contain (string, integer, boolean, or reference to another unit). (Tiers are also sometimes referred to as "features" in the code. The fact that this object has multiple names that both overlap with the names of its attributes is a confusion that should probably be fixed at some point.)
- **Feature Values** are values for a particular tier for a particular unit. They are divided into definite features, which must be unique per tier-unit pair and are associated with a user and can have a confidence indicator (integer), and suggestions, which are not unique and have a probability field.

The database schema is defined in [`schema.sql`](rebabel_format/schema.sql) and the Python interface for it is in [`db.py`](rebabel_format/db.py).

## Processes

Processes are defined in the [`processes`](rebabel_format/processes) directory. To define a new process, create a class which inherits from `Process` ([`process.py`](rebabel_format/process.py)) and has a `name` attribute. Then the `load_processes` function in [`__init__.py`](rebabel_format/__init__.py) will import it and the `MetaProcess` metaclass will automatically register it in `ALL_PROCESSES`. Any class attributes whose values are instances of `Parameter` or its subclasses ([`parameters.py`](rebabel_format/parameters.py)) will be replaced by the typechecked values from the configuration file when the process object is instantiated. The `Process` class includes a parameter named `db` which expects a path to a database file and converts it into an `RBBLFile` object ([`db.py`](rebabel_format/db.py)).

The main action of a process happens in the `.run()` method, which takes no parameters and should not return anything.

## Import and Export

Adding data to a database is done with the `import` process ([`importer.py`](rebabel_format/processes/importer.py)) and taking data from a database and outputting it in a particular file format is done with the `export` process ([`export.py`](rebabel_format/processes/export.py)). Both of these make use of classes defined in the [`converters`](rebabel_format/converters) directory.

Similarly to processes, format conversion classes either inherit from `Reader` ([`reader.py`](rebabel_format/reader.py)) for importing or from `Writer` ([`writer.py`](rebabel_format/writer.py)) for exporting, which in turn have the metaclasses `MetaReader` and `MetaWriter`, which register them in `ALL_READERS` and `ALL_WRITERS` based on the value of the `identifier` attribute.

In general, a file in [`converters`](rebabel_format/converters) should contain both a `Reader` class and a `Writer` class, both for the same format.

The main action of a `Reader` subclass happens in the `.read_file(file)` method, which is passed the file to be read from. If the reader is inheriting directly from `Reader`, this will be an open file handle (which should not be closed). If the reader inherits from `XMLReader`, it will be an [`ElementTree.Element`](https://docs.python.org/3/library/xml.etree.elementtree.html) instance. Descendants of `JSONReader` will be passed a dictionary.

There is also `LineReader`, which is intended for plaintext files where linebreaks are meaningful. Rather than defining a `.read_file(file)` method, subclasses should define `.process_line(line)`, which will be passed a line of the file as a string with preceding and trailing whitespace removed, and `.is_boundary(line)`, which should return `True` if `line` represent a boundary between blocks, such as a sentence boundary (the default definition of this method checks whether the line is empty).

Readers operate on "blocks" of units. While processing a section of a file (or sometimes the entire file), units are referred to by whatever IDs are convenient to the reader object (so long as they can be used as dictionary keys) and information about them is specified with the methods `set_type`, `set_parent`, `add_relation`, and `set_feature`. When a break is reached, the reader calls `finish_block`, which validates the current set of units and writes them to the database, applying any renaming of tiers or unit types that the user specified in the configuration file. Reader objects should generally not touch the database directly to avoid messing up this renaming.

Reader classes should have docstrings which explain what tier names they import.

Writer classes will at some point be similar to this, but currently have stubbed in versions that need to be reworked.

## Querying

There is a query language for this project. It is implemented in [`query.py`](rebabel_format/query.py) and needs to be documented. It can be used for rewrite rules, which are implemented in [`transform.py`](rebabel_format/transform.py).

## Testing

The [`test`](rebabel_format/test) directory is presently setup for end-to-end testing of processes. It should be expanded.

## Other Files

- [`__init__.py`](rebabel_format/__init__.py) defines the command-line interface and some utility functions for importing processes and converters.
- [`config.py`](rebabel_format/config.py) defines functions for processing configuration files.
- [`setup.py`](setup.py) and [`setup.cfg`](setup.cfg) define the Python build system and dependencies (which are exclusively backports of modules not found in all versions of the Python standard library).
- [`.editorconfig`](.editorconfig) specifies [EditorConfig](https://editorconfig.org) formatting instructions. Please ensure that your editor accepts this file.
