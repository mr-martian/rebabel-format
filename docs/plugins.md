# Plugins

Plugins to reBabel are loaded using [entry point specifiers](https://packaging.python.org/en/latest/guides/creating-and-discovering-plugins/#using-package-metadata) in the Python package metadata.

For example, if importing `my_process` would import a subclass of `Process`, then the following should be added to `setup.cfg`:

```cfg
[options.entry_points]
rebabel.processes =
  my_process = my_process
```

The other recognized entry points are `rebabel.readers` for `Reader` instances, `rebabel.writers` for `Writer` instances, and `rebabel.converters` for files which contain both.
