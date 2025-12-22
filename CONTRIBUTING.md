# Contributing to Lance Namespace Implementations

The Lance Namespace Implementations codebase is at [lance-format/lance-namespace-impls](https://github.com/lance-format/lance-namespace-impls).
This repository contains namespace implementations for various catalog systems that integrate with the Lance format.

For contributing changes to the Lance Namespace specification or generated clients,
please go to the [lance-namespace](https://github.com/lance-format/lance-namespace) repo.

For contributing changes to directory and REST namespaces, please go to the [lance](https://github.com/lance-format/lance) repo.

## Repository structure

This repository currently contains the following components:

| Component           | Language | Path                                        | Description                              |
|---------------------|----------|---------------------------------------------|------------------------------------------|
| Glue Namespace      | Java     | java/lance-namespace-glue                   | AWS Glue catalog implementation          |
| Hive2 Namespace     | Java     | java/lance-namespace-hive2                  | Hive Metastore 2.x implementation        |
| Hive3 Namespace     | Java     | java/lance-namespace-hive3                  | Hive Metastore 3.x implementation        |
| Unity Namespace     | Java     | java/lance-namespace-unity                  | Databricks Unity Catalog implementation  |
| Polaris Namespace   | Java     | java/lance-namespace-polaris                | Apache Polaris catalog implementation    |
| Glue Namespace      | Python   | python/src/lance_namespace_impls/glue.py    | AWS Glue catalog implementation          |
| Hive Namespace      | Python   | python/src/lance_namespace_impls/hive.py    | Hive Metastore implementation            |
| Unity Namespace     | Python   | python/src/lance_namespace_impls/unity.py   | Unity Catalog implementation             |

## Development Setup

### Install uv

We use [uv](https://docs.astral.sh/uv/getting-started/installation/) for Python development.
Make sure it is installed.

### Java Development

```bash
cd java
./mvnw install
```

### Python Development

```bash
cd python
uv sync
uv run pytest
```

## Build

Top-level commands available:

- `make clean`: Remove all build artifacts
- `make build`: Build all modules
- `make test`: Run all tests

Language-specific commands:

- `make clean-java`: Clean Java modules
- `make build-java`: Build Java modules
- `make test-java`: Test Java modules
- `make clean-python`: Clean Python modules
- `make build-python`: Build Python modules
- `make test-python`: Test Python modules

## Documentation

The documentation is built using [mkdocs-material](https://pypi.org/project/mkdocs-material).

```bash
cd docs
make serve  # Start local server
make build  # Build static site
```

## Release Process

This section describes the CI/CD workflows for automated version management, releases, and publishing.

### Version Scheme

- **Stable releases:** `X.Y.Z` (e.g., 1.2.3)
- **Preview releases:** `X.Y.Z-beta.N` (e.g., 1.2.3-beta.1)

### Creating a Release

1. **Create Release Draft**
   - Go to Actions -> "Create Release"
   - Select parameters:
     - Release type (major/minor/patch)
     - Release channel (stable/preview)
     - Dry run (test without pushing)
   - Run workflow (creates a draft release)

2. **Review and Publish**
   - Go to the [Releases page](../../releases) to review the draft
   - Edit release notes if needed
   - Click "Publish release" to:
     - For stable releases: Trigger automatic publishing for Java and Python
     - For preview releases: Create a beta release (not published)
