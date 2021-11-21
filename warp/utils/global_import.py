import inspect


class GlobalImport:
    """
    Convenient context manager for lazy importing of modules within a 
    [`Pipe`](../../pipes/pipes/#Pipe) 
    declaration.
    Since the lazily loaded modules are added to `globals()`, you can reference them anywhere in your pipeline runtime so long as the reference occurs after the context manager.


    Example:
        ```python
        # After the next two lines, `os` will be available for reference globally.
        with GlobalImport(globals()):
            import os
        ```
    """
    def __init__(self, gbls :dict):
        """
        Arguments:
            gbls: You must pass `globals()` upon use.
        """
        self.globals = gbls 

    def __enter__(self) -> object:
        return self

    def __call__(self) -> None:
        self.collector = inspect.getargvalues(
            inspect.getouterframes(
                inspect.currentframe())[2].frame
            ).locals

    def __exit__(self, *args) -> None:
        self()
        self.globals.update(self.collector)
