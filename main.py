"""Prefect Horizon entrypoint for a READ-ONLY Garmin Connect MCP server.

Deploy on Horizon with entrypoint  main.py:mcp

Required env (set as Horizon secrets/vars):
  GARMIN_TOKEN_B64        base64 of ~/.garminconnect/garmin_tokens.json — the
                          INITIAL seed token (run `garmin-mcp-auth` locally, then
                          `base64 -w0 ~/.garminconnect/garmin_tokens.json`).
  GARMIN_DISABLED_TOOLS   comma-separated write tools to hide (read-only policy).

Durable token persistence (strongly recommended — without it auth dies within
days, see below):
  UPSTASH_REDIS_REST_URL    Upstash Redis REST endpoint.
  UPSTASH_REDIS_REST_TOKEN  Upstash Redis REST token.
  GARMIN_TOKEN_KV_KEY       optional; key name (default "garmin:token").
  GARMIN_TOKEN_FORCE_SEED   optional; set to "1" to overwrite the KV store from
                            GARMIN_TOKEN_B64 on next boot (use when re-seeding a
                            fresh token). Remove it after one deploy.

Why persistence is needed: Garmin ROTATES the refresh token on every refresh
(garminconnect client.py: `self.di_refresh_token = data.get("refresh_token", ...)`).
Horizon's filesystem is ephemeral and cold-starts revert to the static
GARMIN_TOKEN_B64 snapshot — whose refresh token was already spent by an earlier
refresh — so auth fails a few days after each deploy. Persisting the rotated
token to Upstash lets cold-starts reload the CURRENT token and just refresh it
(no fresh SSO login → no Garmin 429 rate-limit).

This file assembles the upstream tool modules onto a fastmcp v2 server and adds
token persistence; it contains no Garmin API logic, so it stays trivial to rebase
on upstream.
"""

from __future__ import annotations

import base64
import importlib
import os
import pathlib
import sys

import requests

# The package uses a src/ layout. Horizon installs third-party deps from
# requirements.txt but not this local package, so put src/ on the path to make
# `import garmin_mcp` work without a separate install step.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent / "src"))


# ── Upstash Redis (durable token store over HTTP REST) ──────────────────────────
_KV_URL = os.getenv("UPSTASH_REDIS_REST_URL")
_KV_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")
_KV_KEY = os.getenv("GARMIN_TOKEN_KV_KEY", "garmin:token")


def _kv_enabled() -> bool:
    return bool(_KV_URL and _KV_TOKEN)


def _kv_cmd(*args):
    # Upstash REST: POST the command as a JSON array; response is {"result": ...}.
    r = requests.post(
        _KV_URL,
        json=list(args),
        headers={"Authorization": f"Bearer {_KV_TOKEN}"},
        timeout=10,
    )
    r.raise_for_status()
    return r.json().get("result")


def _kv_get():
    try:
        return _kv_cmd("GET", _KV_KEY)
    except Exception as e:  # noqa: BLE001 — store must never crash the server
        print(f"[garmin-mcp kv] GET failed: {e!r}", file=sys.stderr, flush=True)
        return None


def _kv_set(value: str) -> bool:
    try:
        _kv_cmd("SET", _KV_KEY, value)
        return True
    except Exception as e:  # noqa: BLE001
        print(f"[garmin-mcp kv] SET failed: {e!r}", file=sys.stderr, flush=True)
        return False


# ── Materialize the token BEFORE importing garmin_mcp (it reads GARMINTOKENS at
#    import). Prefer the persisted (rotated) token; fall back to the env seed. ────
_b64 = os.getenv("GARMIN_TOKEN_B64")
_force_seed = os.getenv("GARMIN_TOKEN_FORCE_SEED", "").lower() in ("1", "true", "yes")
_dir = pathlib.Path(os.getenv("GARMINTOKENS") or "/tmp/garmintokens")
_dir.mkdir(parents=True, exist_ok=True)
_tok_path = _dir / "garmin_tokens.json"
os.environ["GARMINTOKENS"] = str(_dir)

_token_json = None
_source = None

# 1. Prefer the durable store (the current, post-rotation token).
if _kv_enabled() and not _force_seed:
    _token_json = _kv_get()
    if _token_json:
        _source = "kv"

# 2. Fall back to the static env seed (first boot, empty store, or forced reseed).
if (not _token_json or _force_seed) and _b64:
    try:
        _token_json = base64.b64decode(_b64).decode("utf-8")
        _source = "env-seed"
    except Exception as _e:  # bad base64 → wrong value pasted
        _token_json = None
        _TOKEN_DIAG = (
            f"GARMIN_TOKEN_B64 present ({len(_b64)} chars) but could NOT be "
            f"decoded: {_e!r}. Re-copy the full base64 string."
        )

if _token_json:
    _tok_path.write_text(_token_json, encoding="utf-8")
    # Seed/overwrite the store when we used the env seed so future boots (and
    # other hosts) share the same starting point.
    if _source == "env-seed" and _kv_enabled():
        _kv_set(_token_json)
    _kv_state = (
        "on" if _kv_enabled()
        else "OFF (no persistence; set UPSTASH_REDIS_REST_URL/TOKEN or auth dies "
             "on cold-start)"
    )
    _TOKEN_DIAG = (
        f"token loaded from {_source} ({len(_token_json)} bytes) -> {_tok_path}; "
        f"kv={_kv_state}"
    )
elif not _b64:
    _TOKEN_DIAG = (
        "No token available: GARMIN_TOKEN_B64 is not set and the KV store is "
        f"empty/off (kv={'on' if _kv_enabled() else 'off'}). Seed a token."
    )
# else: decode-error diag already set above.
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
    """Authenticate to Garmin on first use, and persist rotated tokens.

    Import must succeed without credentials (Horizon runs `fastmcp inspect` at
    build time), so login is deferred to the first API call. After each call we
    write the current token back to the KV store, because garminconnect rotates
    the refresh token during refreshes — persisting it is what lets cold-starts
    survive (see module docstring).
    """

    def __init__(self) -> None:
        self._raw = None      # underlying Garmin object (for dumps())
        self._proxy = None    # gm._GarminProxy (surfaces auth/rate errors)
        self._saved = None    # last token string written to the store

    def _ensure(self):
        if self._proxy is None:
            c = gm.init_api(os.getenv("GARMIN_EMAIL"), os.getenv("GARMIN_PASSWORD"))
            if c is None:
                raise RuntimeError("Garmin auth failed. Diagnostic: " + _TOKEN_DIAG)
            self._raw = c
            self._proxy = gm._GarminProxy(c)
            # Loading may have proactively refreshed (rotating the token); persist
            # the current one immediately so the store is never behind.
            self._persist(force=True)
        return self._proxy

    def _token(self):
        try:
            return self._raw.client.dumps()
        except Exception:  # noqa: BLE001
            return None

    def _persist(self, force: bool = False) -> None:
        if not _kv_enabled():
            return
        cur = self._token()
        if cur and (force or cur != self._saved):
            if _kv_set(cur):
                self._saved = cur

    def __getattr__(self, name):
        # Only reached for Garmin API methods (real attrs resolve normally).
        proxy = self._ensure()
        attr = getattr(proxy, name)
        if not callable(attr):
            return attr

        def wrapped(*args, **kwargs):
            result = attr(*args, **kwargs)
            self._persist()  # capture any refresh-rotation from this call
            return result

        return wrapped


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
