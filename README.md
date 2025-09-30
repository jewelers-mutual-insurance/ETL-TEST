# Data Science Repo Template

This repository is designed as a template for data science projects at JM. This template has been modified from [cookie cutter data science](https://github.com/drivendataorg/cookiecutter-data-science), and is designed to be a starting point for a new project, not a comprehensive code structure.

Please rewrite these sections to set up the README file for the project.

# Project Title

Simply describe the project's overview of use/purpose

## Description

An in-depth paragraph about your project and overview of use.

### Directory Structure

```
├── data
│   ├── raw                 <- The original, immutable data dump
│   └── transformed         <- Data that has been transformed or processed
|
├── notebooks               <- Jupyter notebooks for exploration, experimentation, and reports
|
├── src                     <- Source code for use in this project
|   │
|   ├── __init__.py         <- Makes src a Python module
|   │
|   ├── config.py           <- Store useful variables and configuration
|   │
|   ├── ingest.py           <- Scripts to download or generate data
|   │
|   ├── predict.py          <- Code to run model inference with trained models
|   │
|   ├── train.py            <- Code to train models
|   │
|   ├── transform.py        <- Code to clean and transform input data, and create features for modeling
|   │
|   └── utils.py            <- Utility and helper functions that are used throughout the project 
|
├── tests                   <- Source code for tests used in this project
|   │
|   ├── integration.py      <- Integration tests for deploying to a new environment
|   |
|   └── utils.py            <- Unit tests for individual functions
│
├── .gitattributes          <- Config file to specify how Git should treat certain file types
|
├── .gitignore              <- Config file to specify files and folders Git should intentionally track or ignore
|
├── .pre-commit-config.yaml <- Config used for git commit hooks
|
├── .python-version         <- What version of python is required for the project
|
├── pyproject.toml          <- Config file used by package managers like uv and ruff
|
├── README.md               <- The top-level README for developers using this project
|
└── uv.lock                 <- Cross-platform lockfile for project dependencies
```

Instead of a uv lock file, the project could have an environment file to help recreate the environment. This can be a requirements.txt (`pip freeze > requirements.txt`), environment.yml (`conda env export > environment.yml`), or a poetry lock file (if using poetry). 

If your project has secrets or local variables that you will need to use, but don't want to commit to the repository, use a .env file. 

Consider also adding a models folder to save trained and serialized models, model predictions, or model summaries.

## Getting Started

### Dependencies
* By default, this template includes:
    * python 3.11.9
    * ruff (linting and formatting)
    * pre-commit (git commit hooks)
    * pytest (unit testing)

##### Creating your dev virutal environment:
The following is an example of how to create a virtual environment using the uv python package manager.
```
pip install uv
uv init
uv venv
source .venv/Scripts/activate
uv sync # syncs venv with lock file
pre-commit install # installs pre-commit in .git\hooks
```

*Note*: using anaconda requires a license, so is not the preferred method for managing virtual environments.

##### Auto exporting notebooks to python scripts
This helps with code diffs and reviews
```
uv add nbautoexport
nbexport install # do this 1x per machine
nbexport configure notebooks/ 
```

### Installation
* How and where to download the program
* Any modifications needed to be made to files and folders

### Execution
* How to run the program

## Contributors
* Name, Email

## Version History
* 0.2
    * Various bug fixes and optimizations
    * See [commit change]() or See [release history]()
* 0.1
    * Initial Release
