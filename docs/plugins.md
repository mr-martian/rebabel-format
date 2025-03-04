# Modules and Plugins

In reBabel, [readers](readers.md), [writers](writers.md), and [processes](processes.md) can be invoked by name as soon as the file containing them is imported.
A set of utility functions is provided for importing all available modules:

```python
rebabel_format.load_processes(True)
rebabel_format.load_readers(True)
rebabel_format.load_writers(True)
```

The boolean argument to these functions determines whether 3rd-party plugin modules will also be loaded.
A wrapper function `rebabel_format.load()` is provided which invokes all three of these.

## Plugins

Plugins to reBabel are loaded using [entry point specifiers](https://packaging.python.org/en/latest/guides/creating-and-discovering-plugins/#using-package-metadata) in the Python package metadata.

For example, if importing `my_process` would import a subclass of `Process`, then the following should be added to `setup.cfg`:

```cfg
[options.entry_points]
rebabel.processes =
  my_process = my_process
```

The other recognized entry points are `rebabel.readers` for `Reader` instances, `rebabel.writers` for `Writer` instances, and `rebabel.converters` for files which contain both.
