import os
from pathlib import Path
from functools import lru_cache

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - dependency should be installed from requirements
    load_dotenv = None


PROJECT_ROOT = Path(__file__).resolve().parents[2]
APP_ROOT = Path(__file__).resolve().parents[1]
ENV_FILES = [PROJECT_ROOT / ".env", APP_ROOT / ".env"]
PLACEHOLDER_TOKENS = ("USER", "DB_NAME", "YOUR_ENCODED_PASSWORD")
_DOTENV_LOADED = False
_DOTENV_DATABASE_URL = ""
_DOTENV_DATABASE_URL_FILE = ""


def _read_env_file_value(key: str) -> tuple[str, str]:
    for env_file in ENV_FILES:
        if not env_file.exists():
            continue
        for raw_line in env_file.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            name, value = line.split("=", 1)
            if name.strip() != key:
                continue
            return value.strip().strip('"').strip("'"), str(env_file)
    return "", ""


def _load_dotenv_without_override() -> None:
    global _DOTENV_LOADED, _DOTENV_DATABASE_URL, _DOTENV_DATABASE_URL_FILE
    if _DOTENV_LOADED:
        return

    _DOTENV_DATABASE_URL, _DOTENV_DATABASE_URL_FILE = _read_env_file_value("DATABASE_URL")
    if load_dotenv is not None:
        for env_file in ENV_FILES:
            if env_file.exists():
                load_dotenv(env_file, override=False)
    _DOTENV_LOADED = True


def get_database_config() -> dict:
    """Return DATABASE_URL plus provenance. Shell always wins over .env."""
    shell_value = os.environ.get("DATABASE_URL", "").strip()
    if shell_value:
        return {
            "url": shell_value,
            "source": "shell",
            "env_file": None,
        }

    _load_dotenv_without_override()
    loaded_value = os.environ.get("DATABASE_URL", "").strip()
    if loaded_value:
        return {
            "url": loaded_value,
            "source": ".env",
            "env_file": _DOTENV_DATABASE_URL_FILE or None,
        }

    if _DOTENV_DATABASE_URL:
        return {
            "url": _DOTENV_DATABASE_URL,
            "source": ".env",
            "env_file": _DOTENV_DATABASE_URL_FILE or None,
        }

    return {"url": "", "source": "missing", "env_file": None}


def get_database_url() -> str:
    """Return DATABASE_URL, preferring the shell over any .env file."""
    return get_database_config()["url"]


def normalize_database_url(database_url: str | None = None) -> str:
    """Normalize common local URL mistakes before SQLAlchemy parses them."""
    url = database_url or get_database_url()
    if not url or url.count("@") <= 1 or "://" not in url:
        return url

    scheme, rest = url.split("://", 1)
    last_at = rest.rfind("@")
    if last_at <= 0:
        return url

    userinfo = rest[:last_at].replace("@", "%40")
    return f"{scheme}://{userinfo}{rest[last_at:]}"


def is_placeholder_database_url(database_url: str | None = None) -> bool:
    """Return True for documentation/sample URLs that must never be used."""
    url = database_url or get_database_url()
    if not url:
        return False

    if any(token in url for token in PLACEHOLDER_TOKENS):
        return True

    try:
        parsed = make_url(normalize_database_url(url))
    except Exception:
        return False

    return parsed.username == "USER" or parsed.database == "DB_NAME"


def mask_database_url(database_url: str | None = None) -> str:
    """Render a password-safe database URL for logs and UI."""
    url = normalize_database_url(database_url or get_database_url())
    if not url:
        return ""

    try:
        return make_url(url).render_as_string(hide_password=True)
    except Exception:
        if "@" not in url:
            return url
        scheme = ""
        rest = url
        if "://" in url:
            scheme, rest = url.split("://", 1)
            scheme = f"{scheme}://"
        return f"{scheme}****@{rest.rsplit('@', 1)[-1]}"


def get_database_info() -> dict:
    config = get_database_config()
    database_url = config["url"]
    normalized_url = normalize_database_url(database_url)
    if not database_url:
        return {
            "detected": False,
            "source": "missing",
            "env_file": None,
            "masked_url": "",
            "host": None,
            "port": None,
            "database": None,
            "username": None,
            "driver": None,
            "invalid_placeholder": False,
            "placeholder_error": None,
            "error": None,
        }

    try:
        parsed = make_url(normalized_url)
        return {
            "detected": True,
            "source": config["source"],
            "env_file": config["env_file"],
            "masked_url": parsed.render_as_string(hide_password=True),
            "host": parsed.host or "local/socket",
            "port": parsed.port,
            "database": parsed.database,
            "username": parsed.username,
            "driver": parsed.drivername,
            "invalid_placeholder": is_placeholder_database_url(database_url),
            "placeholder_error": (
                "Invalid placeholder DATABASE_URL detected"
                if is_placeholder_database_url(database_url)
                else None
            ),
            "error": None,
        }
    except Exception as exc:
        return {
            "detected": True,
            "source": config["source"],
            "env_file": config["env_file"],
            "masked_url": mask_database_url(normalized_url),
            "host": None,
            "port": None,
            "database": None,
            "username": None,
            "driver": None,
            "invalid_placeholder": is_placeholder_database_url(database_url),
            "placeholder_error": (
                "Invalid placeholder DATABASE_URL detected"
                if is_placeholder_database_url(database_url)
                else None
            ),
            "error": (
                f"{exc}. If your password contains @, encode it as %40 in DATABASE_URL."
            ),
        }


def has_database_config() -> bool:
    """Return True when DATABASE_URL is configured."""
    return bool(get_database_url())


@lru_cache(maxsize=1)
def _get_engine(database_url: str):
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set.")
    return create_engine(normalize_database_url(database_url), pool_pre_ping=True, future=True)


def get_engine():
    return _get_engine(get_database_url())


def read_table_or_query(query: str) -> pd.DataFrame:
    """Read a SQL query into a DataFrame."""
    if not has_database_config():
        raise RuntimeError("DATABASE_URL is not set.")
    if is_placeholder_database_url():
        raise RuntimeError("Invalid placeholder DATABASE_URL detected.")
    with get_engine().connect() as connection:
        return pd.read_sql(text(query), connection)


def test_connection():
    """Return a tuple of (connected, message) for the configured database."""
    if not has_database_config():
        return False, "DATABASE_URL is not set."
    if is_placeholder_database_url():
        return False, "Invalid placeholder DATABASE_URL detected."

    try:
        with get_engine().connect() as connection:
            connection.execute(text("SELECT 1"))
        return True, "Connection successful."
    except Exception as exc:  # pragma: no cover - depends on local DB state
        return False, str(exc)
