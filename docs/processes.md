# Processes

Creating a new process can be done by subclassing `Process`.

```python
class SomeProcess(Process):
    name = 'do_stuff'

    amount = Parameter(type=int, default=1, help='amount of stuff to do')

    def run(self):
        for i in range(self.amount):
            print('doing stuff to', self.db.path)
```

To be invokable, a process must have a `name` attribute. The main action of the process occurs in the `run` method, which takes no arguments.

The [parameters](parameters.md) that the process expects are specified by adding attributes of type `Parameter`. When the class is initialized, these are converted into the appropriate values. Subclasses of `Process` also inherit a required parameter named `db` which will have an `RBBLFile` as a value.

Processes can be defined either in the `processes` directory of the reBabel project or in [plugins](plugins.md) which declare a `rebabel.processes` entry point.
