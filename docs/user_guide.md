# User Guide

WARP stands for Workspace Allowing Reproducible Pipelines.
This page will serve as a reference for how to use WARP in practice.
It contains a [tutorial](./#full-tutorial).

---

## Introductory Tutorial

In this tutorial, we will create a minimal pipeline that will guide you through the functionality of WARP.
The pipeline itself is fairly abstract, intending to reflect structural details one might encounter in a machine learning workflow rather than the implementation minutia.

The basic idea of WARP is to consider your pipeline as a Directed Acyclic Graph (DAG) in which edges represent pieces of data and vertices represent functions that operate on those pieces of data.

There are three core classes you need to understand in order to use WARP:

1. [`Pipe`](../../pipes/pipes/#pipe): The core functional unit of WARP -- a chunk of code that takes in pieces of data and outputs (new) pieces of data. 
    Pipes behave like relatively self-contained functions that explicitly declare what data they depend on and what data they produce, thereby implicitly defining a DAG.
    The user will wrap their pipeline functionality with these pipe classes.

2. [`Graph`](../../graph/graph/#graph): The data structure that formally connects pipes together into a DAG.
    The user will rarely interact directly with this object beyond instantiating it and passing it to [`Workspace`](../../workspace/workspace/#workspace).

3. [`Workspace`](../../workspace/workspace/#workspace): An API for the user to interact with the graph that keeps track of metadata and data provenance.

### Using [`Pipe`](../../pipes/pipes/#pipe)

???+abstract
    This section demonstrates the following concepts:
    
    - [`Source`](../../pipes/pipes/#source) pipes -- how to ingest external data artifacts.
    - [`Pipe`](../../pipes/pipes/#pipe) subclasses -- how to define data processing functionality.
    - The [`@dependencies`](../../data/data/#dependencies) decorator -- how to declare which pieces of data the pipe ingests.
    - The [`@produces`](../../data/data/#produces) decorator -- how to declare which pieces of data the pipe outputs for ingestion by other pipes.
    - TODO

In this section, we will create two pipes.
The full pipeline used in this tutorial contains additional pipes, which you can find [TODO].

The first thing you must do is encapsulate your pipeline data processing code into a [`Pipe`](../../pipes/pipes/#pipe) subclass.

!!!important
    You can only declare one [`Pipe`](../../pipes/pipes/#pipe) subclass per file.

!!!example
    In this example, we create a [`Pipe`](../../pipes/pipes/#pipe) subclass that will ingest a dataset and output a new preprocessed dataset.
    The next example creates a downstream pipe that relies on this one.
    ```python
    TODO
    ```


## Advanced Tutorial
