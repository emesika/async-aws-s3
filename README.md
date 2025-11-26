# AWS-S3 / Async GitLab Utility

A lightweight **async Python utility** for interacting with GitLab using `gidgetlab` + `aiohttp`, packaged with **PDM** and running on **Python 3.12+**.

This project includes:

- A fully async script (`get_gitlab_project.py`) that:
  - Authenticates using environment variables  
  - Fetches GitLab project metadata  
  - Lists **protected branches** (excluding `main`)  
- A modern `pyproject.toml` that defines dependencies, dev-tools, and PDM entrypoints.  
- Optional FastAPI integration for future S3-related API work.

## Features

### ✔ Async GitLab Access  
Using `gidgetlab.aiohttp.GitLabAPI` and `aiohttp` sessions.

### ✔ Protected Branch Discovery  
Automatically filters:
- non-protected branches  
- the `main` branch

### ✔ Clean, Minimal Project Setup  
Configured with:
- **PDM** for dependency management  
- **Ruff** for linting  
- **pytest / pytest-asyncio** for testing  
- **httpx** for async HTTP testing

## Installation

### Install PDM

```bash
pip install pdm
```

### Install dependencies

```bash
pdm install
```

### Activate virtual environment

```bash
pdm venv activate
```

## Environment Variables

| Variable | Description |
|---------|-------------|
| GITLAB_SERVER | Base URL |
| GITLAB_PROJECT_ID | Numeric project ID |
| GITLAB_TOKEN | GitLab token |

Example:

```bash
export GITLAB_SERVER="https://mygitlab.mydomain.com"
export GITLAB_PROJECT_ID=1234
export GITLAB_TOKEN="your_token_here"
```

## Usage

Run the GitLab tool:

```bash
pdm run python src/get_gitlab_project.py
```

or:

```bash
python3 src/get_gitlab_project.py
```

## Development

### FastAPI dev server

```bash
pdm run dev
```

### Tests

```bash
pdm run pytest -vv
```

### Lint

```bash
pdm run ruff check .
```

## License

MIT License

## Author

Eli Mesika <elimesika@gmail.com>
