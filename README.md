# rebabel-format
Python library for interacting with reBabel data files

## Installation

To install this package locally, run

```bash
$ pip3 install -e .
```

## Usage

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
