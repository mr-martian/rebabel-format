# rebabel-format
Python library for interacting with reBabel data files

## Installation

To install this package locally, run

```bash
$ pip3 install -e .
```

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
features = [{tier = "UD", feature = "upos", value = "NOUN"}]
# N is part of S
parent = "S"

[query.V]
# V is a word
type = "word"
# V is part of S
parent = "S"

# a different way of listing features
[[query.V.features]]
tier = "UD"
feature = "upos"
value = "VERB"

[[query.V.features]]
tier = "UD/FEATS"
feature = "Person"
value = "3"
```

## Library Usage

### Loading Modules

Readers, writers, and processes are imported dynamically at startup:

```python
rebabel_format.load_processes(True)
rebabel_format.load_readers(True)
rebabel_format.load_writers(True)
```

Each function takes a single parameter indicating whether or not plugins outside this module should be loaded.

Plugin packages can be created using [entry point specifiers](https://packaging.python.org/en/latest/guides/creating-and-discovering-plugins/#using-package-metadata) in the package metadata. The entry point can indicate either a module or a class (any subclass of `Process`, `Reader`, or `Writer` is automatically registered on import).

The loading functions check for the following plugin namespaces: `rebabel.processes`, `rebabel.converters`, `rebabel.readers`, `rebabel.writers`.

After loading, lists of available names can be retrieved as follows:

```python
rebabel_format.get_process_names()
rebabel_format.get_reader_names()
rebabel_format.get_writer_names()
```

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
    # use UD:lemma where FlexText wants FlexText/en:txt
    {'in_feature': 'UD:lemma', 'out_feature': 'FlexText/en:txt'},
  ],
  # settings specific to the FlexText writer:
  # the highest non-empty node will be the phrase
  # (the CoNLL-U importer currently doesn't create paragraph and document nodes)
  root='phrase',
  # the morpheme layer will also be empty
  skip=['morph'],
)
```
