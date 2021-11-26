# WARP ![](./artifacts/warp_pipe.png) 

## What is WARP?

WARP (Workspace Allowing Reproducible Pipelines) is a small-scale workflow orchestration tool designed 
in a data-driven fashion to help data scientists and ML researchers organize their research code in a 
reproducible manner. 
WARP's is most similar to [Metaflow](https://metaflow.org/) but without the aim of scaling projects 
to cloud services like AWS.
Instead, WARP targets a more localized setting e.g. ML research on your own GPU box.

WARP organizes code into pipes that take in some data and output some new data.
Pipes can be linked together -- WARP automatically makes sure that the outputs of upstream pipes are
chronologically in sync with downstream pipes.
This allows the scientist to only run portions of the pipeline they are actively developing without
worrying that, say, their training data for a model will become out-of-sync.
This helps reduce a significant source of bugs in data science and ML code.

WARP assists with reproducibility by allowing the user to specify hyperparameters for a pipe.
These hyperparameters can be dynamically loaded from config files when a pipe is run and are 
automatically logged in an organized manner.
This way, the user can easily inspect a historical run of the pipeline, see which version of the code
was used, and see which parameters (and their values) were used in the run.

WARP also provides an interactive workspace in which the user can inspect their pipeline and run pipes as
needed.

## What isn't WARP?

WARP is not a replacement for production pipeline orchestration tools like Apache Airflow.
WARP does not handle job scheduling or parallelizing your code.

## Installation

Clone this repo, `cd` into this directory, and run `pip install -e .`

You will need to install graphviz if you want to use the `WorkSpace.view` function to visualize your pipeline.

## Usage
See the [example](examples/basic/) 
for a runnable example that showcases WARP's functionality.

## References

This project could someday belong on a list such as this one:
https://github.com/pditommaso/awesome-pipeline
