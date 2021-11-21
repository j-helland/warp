# Data

Decorators that implicitly define the pipeline graph by ingesting upstream products that a [`Pipe`](../../pipes/pipes/#Pipe) subclass depends on as well as products that the pipe will produce for downstream pipes to ingest themselves.

These must be used for WARP to properly construct a graph of your pipeline.

---

## `@dependencies`

:::warp.data.dependencies
    handler: python

---

## `@produces`

:::warp.data.produces
    handler: python

---

## `Decorator`

:::warp.data.decorator.Decorator
    handler: python