# Dependency Management

Runtime dependencies live in `requirements.txt` and are exact direct pins. Development-only tools live in
`requirements-dev.txt` so NAS deployment can stay small.

## Install

Runtime only:

```powershell
python -m pip install -r requirements.txt
```

Development:

```powershell
python -m pip install -r requirements.txt -r requirements-dev.txt
```

## Audit

Run:

```powershell
python -m pip_audit -r requirements.txt
```

GitHub Actions runs the same audit for pushes and pull requests.

## Update Flow

1. Update one direct dependency pin at a time unless a security fix requires a batch.
2. Run `python -m pip install -r requirements.txt -r requirements-dev.txt`.
3. Run `python -m unittest discover -s hec/tests`.
4. Run `python -m pip_audit -r requirements.txt`.
5. Record behavior changes, config changes, and tests in the pull request.

## Lint And Format

Ruff configuration is in `pyproject.toml`. The current CI gate is intentionally light and checks undefined names. Use
Ruff locally for focused cleanup when touching a file, but avoid broad style-only churn in behavior branches.
