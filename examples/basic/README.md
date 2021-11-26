# WARP example workflow

See [example.ipynb](./example.ipynb) 
for a jupyter notebook tutorial on using WARP.
Much of this documentation is presented there in a more narrative form.

`example.py` shows how to use WARP to manage a project involving a collection of
python scripts with associated input and output files.

## Installation

This directory is provided as a standalone python package to demonstrate the functionality of WARP.
Thus, you need to install this package by doing the following:
- Create a virtual environment (e.g. `python -m venv venv`) in a desired location and activate it 
(e.b. `source venv/bin/activate`).
- Make sure that WARP is installed by `cd`-ing into the warp directory and doing `pip install -e .`
- Install the example by `cd`-ing into this directory and running `pip install -e .`
- Activate the installed jupyter kernel e.g. `python -m ipykernel install --user --name=warp_example`

To verify your installation, run `bash launch` in this directory.
You should see output resembling
```
Your workspace is now loaded as `ws`. 
Do `ws.nodes()` to see pipes in the graph. 
Do `ws.methods()` to see available commands.
Home: .warp
Example command: `ws.backfill('warp.example.D')`
>>>
```

## Defining a pipe

Pipes in WARP are defined as classes.
Let's jump right into an example pipe that creates two output files containing the same string value.
```python
class Main(Pipe):
    message = Parameter('text')
    number = Parameter('some value', default=3)

    def run(self):
        with open('data/A1.txt', 'w') as f:
            f.write(self.message)
        with open('data/A2.txt', 'w') as f:
            f.write(self.message)
```
For WARP to recognize your pipe, you must name it `Main` and subclass the `Pipe` class.
Each pipe must also define a `run` function -- WARP will throw an error otherwise.

For this pipe, we defined a parameter called `message` using the `Parameter` class interface and
a parameter called `number` (that isn't used) containing an integer.
For each parameter, you can define a default value that the parameter will take on at runtime using
the `default` keyword argument -- `message` does not define a default value since it only contains a
single string value. 
WARP allows you to input non-default values at runtime -- see below in the *Running the graph* 
section.

Pipe parameters are intended to be user-specified values that are necessary for reproducing the 
output of the pipe.
`Parameter` is intended as a wrapper for simple values like strings or numbers.
As long as your parameters are attributes of your pipe class, WARP will automatically detect them
and log their values for you.

## Creating your first data node

WARP is currently organized around data *filepaths*.
A node is a pipe that takes in pieces of data, operates on them, and then outputs new pieces of data.
Nodes are defined in terms of their inputs and outputs, as shown in `example.py`:

```python
pg.add(
    pipe=A,
    products=['data/A1.txt', 'data/A2.txt'],
)
```

`data/A1.txt` and `data/A2.txt` are the relative paths of files that will be created by the pipe `example.A`.
In this case, the pipe has no data dependencies, so the argument `parent_products` is not specified.

A valid pipe is a python object that has a `main` method; WARP ultimately calls each pipe as `pipe.main()`. 
As exemplified with [example.A](https://code.sei.cmu.edu/bitbucket/users/jwhelland/repos/warp/browse/example/example/A.py), 
it is recommended to create a separate file for each pipe with `def main() ...` as its final code block.
The intent is to keep data operations separated and modular.

`docstring` is an optional argument that can be useful for keeping track of the big
picture in the context of the list of all nodes in the pathgraph.

## Creating a node with parents

The main use case for WARP is to keep track of complex dependencies between data files and their dependencies. 
In particular, a pipe might depend on the output(s) of some previous pipe.
To handle such dependencies in WARP, you only need to specify the required products via `parent_products`; WARP will
automatically recognize the associated parent pipe since product file paths must be necessarily distinct.
This behavior is demonstrated in `example.py`:
```python
pg.add(
    products='data/B.txt',
    parent_products='data/A1.txt',
    pipe=B,
)
```
Note that multiple parent products are acceptable as a `list` object.

## Inspecting the graph

You can visually inspect the graph by running `ws.view()`.
You can get a list of available pipes in the graph by calling `ws.pipes()`.
Calling `ws.methods()` lists the available methods for the workspace.
You can get a timestamp for when a pipe was last run using `ws.status('example.A')`.

## Running the graph

WARP is designed to be run interactively in a python session. 
A good way to do this is to execute `python -i example.py` at the command line. 
This executes the content of `example.py` and then drops you into an interactive session such that you now can access the workspace and see what methods it contains as `dir(ws)`.

For example, before executing any of the pipes, we can observe that a pipe
is not yet executed as follows:
```python
ws.status('example.D')
```

To build `D` and all of its upstream pipes (in the correct order), simply do
```python
ws.backfill('example.D')
```
`backfill` is idempotent in the sense that nothing happens if the output path already exists and 
all ancestors exist in the correct timestamp order. 
To rebuild `D` (but none of its ancestors), call `ws.build('example.D')`.

### Non-default parameter values
When you run the graph (see below for details), WARP allows you to insert non-default values for
your parameters.
This can be done in two ways:
1. By specifying keyword arguments when calling `build`.
For example, calling `ws.build('example.A', message='new message')` will update the parameter
value for `message` from `'text'` to `'new text'`.
Note that the updated value will be logged with the pipeline run.
2. By config file.
A config file for a pipe can be either a JSON or YAML file whose name is the same as the pipe.
For example, we could create a `A.yml` file with the following contents:
```yaml
# A.yml
message: new text
number: 5
```
This file can be stored in the same directory as its corresponding pipe file `A.py` or can be kept
in a separate directory specified using the `config_dir` keyword argument of the `WorkSpace` class.
WARP's default behavior is to automatically search for these config files; you can disable this
behavior by setting `autoload_parameters=False` in the `WorkSpace` class.

### Resuming a session (UNDER DEVELOPMENT)
You can resume an existing session by passing its ID number to the `session_id` argument of the 
`WorkSpace` class.
This will allow you to more easily inspect the results of a historical session.

## Logging metadata

WARP organizes its metadata into sessions -- when you launch a session, a unique ID is generated
and can be used to find the results of the session later.
The default behavior of WARP is to automatically log the following information within a session:
- The session ID number.
- The version of the code used (via the git commit hash).
- Timestamps for when each pipe was last run.
- The `Parameter` values for each pipe.

## Details

- Nodes may not be nested within each other, `PathGraph.add` enforces this.
- WARP enforces child-parent order in the construction of a node graph. You can't insert
a child before you insert all of its parents.
- WARP internally generates timestamps (i.e. to notice when a child was created before its parent) which are
independent of the file system timestamps.
If you modify a file outside of WARP's workspace, you could silently invalidate the graph.
For this reason, it is recommended that you only modify pipe product files through the mechanisms provided by WARP.
- WARP relies on the user ensure that the filepaths indicated by the inputs/outputs of a pipe agree with 
the files involved in the corresponding pipe.
