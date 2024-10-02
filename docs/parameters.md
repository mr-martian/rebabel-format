# Parameters

[Processes](processes.md), [Readers](readers.md), and [Writers](writers.md) can all be parameterized. To add a parameter to any of these classes, simply define a class attribute of type `Parameter`:

```python
class SomeWriter(Writer):
    warn_on_invalid = Parameter(
      type=bool, default=False,
      help="issue warnings on feature values that don't match the spec",
    )
```

This parameter can then be passed either from a TOML file:

```toml
[export]
# ...
warn_on_invalid = true
```

Or from a Python script:

```python
rebabel_format.run_command(
  'export', #...
  warn_on_invalid=True,
)
```

In either case, methods on `SomeWriter` can simply refer to `self.warn_on_invalid`, which from their perspective will be a boolean.

## `Parameter`

`Parameter` objects have the following attributes:

- `required`: whether to raise an error if this parameter is omitted; defaults to `True`
- `default`: the value of this parameter if not specified by the user; if this is not `None`, then `required` will be set to `False`
- `type`: the type that a provided value must be (according to `isinstance`)
- `help`: the documentation string for this parameter

## `QueryParameter`

Input to this parameter should be a dictionary which is checked to ensure that it is a valid query.

## `UsernameParameter`

A string parameter which is optional by default. If not provided, it will be set to the value of the environment variable `$USER`.

## `DBParameter`

The input to this parameter is expected to be a path to a database. The value will be either `None` or an instance of `RBBLFile`. The parameter `db` of this type is inherited from `Process` and thus directly referring to `DBParameter` is rarely necessary.
