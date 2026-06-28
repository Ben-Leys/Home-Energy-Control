# Repository Guidelines

## Project Structure & Module Organization

This Python app lives under `hec/`. `hec/core/` holds state, models, logging, API server, tariffs, and initialization.
Integrations are in `hec/controllers/` and `hec/data_sources/`. Scheduling and optimization are in
`hec/logic_engine/`; persistence in `hec/database_ops/`; reporting in `hec/reporting/`; dashboard code in
`hec/ui/hec_dashboard.py`. Tests are in `hec/tests/`; assets include `hec/core/vue_dashboard.html` and
`hec/tariffs.yaml`.

## Build, Test, and Development Commands

Create and populate a virtual environment before local work:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

Run the test suite with:

```powershell
python -m unittest discover -s hec/tests
```

Run the controller app from the repository root so package imports resolve:

```powershell
python -m hec.main
```

Run the Streamlit dashboard separately with:

```powershell
streamlit run hec/ui/hec_dashboard.py
```

## Coding Style & Naming Conventions

Use standard Python style: 4-space indentation, `snake_case` for modules, functions, variables, and tests, and
`PascalCase` for classes and enums. Group imports as standard library, third-party, then local `hec` imports. No project
formatter is configured, so keep edits PEP 8 compatible and consistent with nearby code.

## Testing Guidelines

Tests use `unittest`. Name files `test_*.py`, classes `Test...`, and methods `test_<behavior>`. Use `unittest.mock`
for hardware, API, scheduler, and database dependencies. Add focused tests for `hec/logic_engine/`, `hec/core/`, and
API/data-source parsing changes.

## Commit & Pull Request Guidelines

Recent commits are short, often prefixed with `Bug:` or `Update:`. Examples: `Bug: handle missing EVCC charge current`
or `Update: adjust capacity tariff limit`. Pull requests should describe behavior changes, list tests run, mention
required `config.yaml` or `.env` changes, and include dashboard screenshots for UI changes.

## Security & Configuration Tips

Do not commit secrets, local databases, logs, `.env`, or `config.yaml`; these are intentionally ignored. Runtime config
loads from `hec/config.yaml` and `hec/.env`. Keep API keys such as `ENTSOE_API_KEY` in the environment file.

## Agent-Specific Instructions

For token-expensive, non-sensitive helper tasks, agents may call the local Ollama models through Aider so the helper can
inspect repository files. This only works on this desktop PC, not on other devices.

```powershell
aider --model ollama_chat/qwen3.6:35b --no-show-model-warnings --no-auto-commits --yes-always --read hec\path\to\file.py --message "<task prompt>"
```

`qwen3-coder:30b` can also be used:

```powershell
aider --model ollama_chat/qwen3-coder:30b --no-show-model-warnings --no-auto-commits --yes-always --read hec\path\to\file.py --message "<task prompt>"
```

Use `qwen3-coder:30b` first for code-focused repo inspection or implementation because it gives terser answers. Use
`qwen3.6:35b` for broader reasoning or vision-capable tasks; it may emit a long `THINKING` block.

Use `--read` for read-only inspection. Pass files as positional arguments only when the helper should be allowed to edit
them. Keep `--no-auto-commits`, inspect any generated diff before accepting it.
