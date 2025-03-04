# rebabel-format

reBabel is a library for converting between various language data files, such as ELAN `.eaf` files, Fieldworks Language Explorer `flextext` files, Universal Dependencies `.conllu` files, and others.
Rather than create a separate converter for each pair of formats, it goes through an intermediary representation using a SQLite database and thus needs only one importer and one exporter for each file format.

reBabel also provides other functionality, including merging data from different sources (say, from manual annotation and from a machine learning model), and a query and rewrite system.

## Installation

To install this package locally, run

```bash
$ pip3 install -e .
```

This package is written in pure Python and has no external dependencies apart from backports of standard library modules to older Python versions.

## Command-Line Usage

This package installs a command-line utility named `rebabel-format`, which can be invoked as follows:

```bash
$ rebabel-format ACTION config.toml
```

Common actions include `import`, `export`, `query`, and `transform`. Run `rebabel-format --help` for a complete list of available actions.

## The Configuration File

Configuration for the various actions is provided in a [TOML](https://toml.io/en/) file.

The parameters to a given action can be top-level keys or they can be under the name of the action, allowing a single config file to be used for multiple steps of a given workflow.

```toml
# the same database file will be used for all workflows
db = "demo.db"

# these next parameters only apply to the "import" action
[import]
mode = "conllu"
infiles = ["file1.conllu", "file2.conllu"]

# to write a query, we define some nodes
# in this case, S, N, and V
[query.S]
# S is a sentence
type = "sentence"

[query.N]
# N is a word
type = "word"
# N has a feature named UD:upos with the value NOUN
features = [{feature = "UD:upos", value = "NOUN"}]
# N is part of S
parent = "S"

[query.V]
# V is a word
type = "word"
# V is part of S
parent = "S"

# a different way of listing features
[[query.V.features]]
feature = "UD:upos"
value = "VERB"

[[query.V.features]]
feature = "UD:FEATS:Person"
value = "3"
```

## Library Usage

### Loading Modules

The modules which implement each supported file format and action can be imported individually, but there is also a helper function which imports all of them.

```python
rebabel_format.load()
```

In most cases, this should be one of the first lines in a script or in an interactive shell.
For finer-grained control over the process, see [the documentation](plugins.md).

### Inspecting Parameters

Different processes and format converters take different arguments. These arguments can be programmatically inspected using the following functions:

```python
rebabel_format.get_process_parameters('import')
rebabel_format.get_reader_parameters('conllu')
rebabel_format.get_writer_parameters('flextext')
```

Each of these returns a dictionary with parameter names as keys and `Parameter` objects as values. `Parameter` objects have the properties `required`, `default`, `type`, and `help`. Any missing parameter which has `required=True` will cause `ValueError` to be raised.

### Invoking Processes

Processes can be invoked as follows:

```python
# import in.conllu into temp.db
rebabel_format.run_command('import', mode='conllu', db='temp.db',
                           infiles=['in.conllu'])

rebabel_format.run_command(
  # export out.flextext from in.db
  'export', mode='flextext', db='temp.db', outfile='out.flextext',
  # making some adjustments to account for differences between
  # CoNLL-U and FlexText
  mappings=[
    # use CoNLL-U sentence nodes where FlexText expects phrases
    {'in_type': 'sentence', 'out_type': 'phrase'},
    # use UD:lemma where FlexText wants FlexText:en:txt
    {'in_feature': 'UD:lemma', 'out_feature': 'FlexText:en:txt'},
  ],
  # settings specific to the FlexText writer:
  # the highest non-empty node will be the phrase
  # (the CoNLL-U importer currently doesn't create paragraph and document nodes)
  root='phrase',
  # the morpheme layer will also be empty
  skip=['morph'],
)
```
