import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


@dataclass(frozen=True)
class IGAccount:
    username: str
    password: str
    api_key: str
    acc_type: str = "LIVE"


@dataclass(frozen=True)
class GmailConfig:
    send_usr: str
    send_pwd: str
    receive_usr_list: List[str]
    email_server: str = "smtp.gmail.com"
    email_port: int = 587


def _load_dotenv_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _load_dotenv_files() -> None:
    here = Path(__file__).resolve().parent
    candidates = [
        here.parent / ".env",
        here / ".env",
        Path.cwd() / ".env",
    ]

    seen = set()
    for path in candidates:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        _load_dotenv_file(resolved)


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _split_recipients(value: str) -> List[str]:
    recipients = []
    for part in value.replace(";", ",").split(","):
        item = part.strip()
        if item:
            recipients.append(item)
    return recipients


def get_ig_account(default_profile: Optional[str] = None) -> IGAccount:
    _load_dotenv_files()
    profile = os.getenv("IG_PROFILE", default_profile or "ACCOUNT1").upper()
    prefix = f"IG_{profile}_"

    return IGAccount(
        username=_required_env(prefix + "USERNAME"),
        password=_required_env(prefix + "PASSWORD"),
        api_key=_required_env(prefix + "API_KEY"),
        acc_type=os.getenv(prefix + "ACC_TYPE", "LIVE"),
    )


def get_gmail_config() -> GmailConfig:
    _load_dotenv_files()
    recipients = _split_recipients(_required_env("GMAIL_RECIPIENTS"))
    if not recipients:
        raise RuntimeError("GMAIL_RECIPIENTS must contain at least one recipient")

    return GmailConfig(
        send_usr=_required_env("GMAIL_USER"),
        send_pwd=_required_env("GMAIL_APP_PASSWORD"),
        receive_usr_list=recipients,
        email_server=os.getenv("GMAIL_SERVER", "smtp.gmail.com"),
        email_port=int(os.getenv("GMAIL_PORT", "587")),
    )
