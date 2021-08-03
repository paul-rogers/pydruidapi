# pydruidapi
Python wrapper for the Druid REST API.

The [pydruid](https://pypi.org/project/pydruid/) is a great tool for running Druid queries
from Python, but it does not cover the full Druid REST API. This project provides those
operations which `pydruid` does not. You can obtain a `pydruid` client directly from
the Druid client provided in this project.

At the moment, the primary purpose of this project is to prototype tools for helping
to optimize a Druid datasource. See the Jupyter nodebooks for examples.

* [Schema analysis](https://nbviewer.jupyter.org/github/paul-rogers/pydruidapi/blob/main/Schema.ipynb) shows how to analyze your table schema.
* (More to come later.)
