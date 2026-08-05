"""Prefect Horizon entrypoint for a READ-ONLY Garmin Connect MCP server.

Deploy on Horizon with entrypoint  main.py:mcp

Required env (set as Horizon secrets/vars):
  GARMIN_TOKEN_B64      base64 of your ~/.garminconnect/garmin_tokens.json
                        (run `garmin-mcp-auth` locally once, then
                         `base64 -w0 ~/.garminconnect/garmin_tokens.json`)
  GARMIN_DISABLED_TOOLS comma-separated write tools to hide (read-only policy)

This file only assembles the upstream tool modules onto a fastmcp v2 server;
it contains no Garmin logic of its own, so it stays trivial to rebase on
upstream.
"""

from __future__ import annotations

import base64
import importlib
import os
import pathlib
import sys

# The package uses a src/ layout. Horizon installs third-party deps from
# requirements.txt but not this local package, so put src/ on the path to make
# `import garmin_mcp` work without a separate install step.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent / "src"))

# The token must exist on disk BEFORE importing garmin_mcp, which reads
# GARMINTOKENS at import time. Materialize it from the env secret.
# _TOKEN_DIAG records exactly what happened so auth failures are diagnosable
# from the error message surfaced to Claude (no log-diving needed).
_b64 = os.getenv("GARMIN_TOKEN_B64")
if not _b64:
    _TOKEN_DIAG = (
        "GARMIN_TOKEN_B64 is NOT set in this deployment's environment. "
        "Add it as a RUNTIME env var/secret on the deployment serving this URL "
        "(check the exact name — GARMIN_TOKEN_B64), then redeploy."
    )
else:
    try:
        _dir = pathlib.Path(os.getenv("GARMINTOKENS") or "/tmp/garmintokens")
        _dir.mkdir(parents=True, exist_ok=True)
        _tok_path = _dir / "garmin_tokens.json"
        _tok_path.write_text(
            base64.b64decode(_b64).decode("utf-8"), encoding="utf-8"
        )
        os.environ["GARMINTOKENS"] = str(_dir)
        _TOKEN_DIAG = (
            f"GARMIN_TOKEN_B64 present ({len(_b64)} chars); wrote "
            f"{_tok_path} ({_tok_path.stat().st_size} bytes). If auth still "
            "fails, the token is stale/corrupt — re-mint and update the secret."
        )
    except Exception as _e:  # bad base64 / undecodable → wrong value pasted
        _TOKEN_DIAG = (
            f"GARMIN_TOKEN_B64 present ({len(_b64)} chars) but could NOT be "
            f"decoded/written: {_e!r}. The pasted value is corrupt/truncated — "
            "re-copy the full base64 string."
        )
print("[garmin-mcp token] " + _TOKEN_DIAG, file=sys.stderr, flush=True)

from fastmcp import FastMCP  # noqa: E402  (Horizon-native server type)
import garmin_mcp as gm  # noqa: E402
from garmin_mcp import _ToolFilter, workout_templates  # noqa: E402

_MODULE_NAMES = [
    "activity_management", "health_wellness", "user_profile", "devices",
    "gear_management", "weight_management", "challenges", "training",
    "workouts", "data_management", "womens_health", "nutrition",
    "workout_builders", "courses", "activity_analysis",
]
_MODULES = [importlib.import_module(f"garmin_mcp.{n}") for n in _MODULE_NAMES]


class _LazyGarmin:
    """Authenticate to Garmin on first use, not at import.

    Horizon runs `fastmcp inspect main.py:mcp` during the build to read tool
    schemas; that import must succeed without the Garmin token secret (which may
    only be present at runtime). Registering tools never touches the client, so
    we defer login until the first actual API call.
    """

    def __init__(self) -> None:
        self._real = None

    def _client(self):
        if self._real is None:
            c = gm.init_api(os.getenv("GARMIN_EMAIL"), os.getenv("GARMIN_PASSWORD"))
            if c is None:
                raise RuntimeError("Garmin auth failed. Diagnostic: " + _TOKEN_DIAG)
            self._real = gm._GarminProxy(c)
        return self._real

    def __getattr__(self, name):
        # Only reached for Garmin API methods (real attrs resolve normally),
        # so any tool call triggers a lazy login here.
        return getattr(self._client(), name)


def _build() -> FastMCP:
    client = _LazyGarmin()
    for module in _MODULES:
        module.configure(client)

    server = FastMCP("Garmin Connect")
    # _ToolFilter honours GARMIN_ENABLED_TOOLS / GARMIN_DISABLED_TOOLS (parsed
    # by garmin_mcp at import) and skips registration of filtered tools.
    app = _ToolFilter(server, gm.enabled_tools, gm.disabled_tools)
    for module in _MODULES:
        app = module.register_tools(app)
    workout_templates.register_resources(app)
    return server


mcp = _build()
