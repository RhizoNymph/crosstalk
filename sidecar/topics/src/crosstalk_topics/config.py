"""Configuration from the environment (docs/features/topics_sidecar.md)."""

import logging
from collections.abc import Mapping
from dataclasses import dataclass

_LEVELS: dict[str, int] = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}


class ConfigError(Exception):
    """An environment variable holds a value the sidecar cannot use."""

    def __init__(self, variable: str, value: str, expected: str) -> None:
        super().__init__(f"{variable}={value!r}: expected {expected}")
        self.variable = variable
        self.value = value
        self.expected = expected


@dataclass(frozen=True, slots=True)
class Config:
    host: str = "0.0.0.0"
    port: int = 8090
    log_level: int = logging.INFO
    max_body_bytes: int = 1 << 30
    layout_cache: int = 4

    @classmethod
    def from_env(cls, environ: Mapping[str, str]) -> Config:
        default = cls()
        return cls(
            host=environ.get("CROSSTALK_TOPICS_HOST", default.host),
            port=_int(environ, "CROSSTALK_TOPICS_PORT", default.port, 1, 65535),
            log_level=_level(environ, "CROSSTALK_TOPICS_LOG_LEVEL", default.log_level),
            max_body_bytes=_int(
                environ, "CROSSTALK_TOPICS_MAX_BODY_BYTES", default.max_body_bytes, 1, 1 << 62
            ),
            layout_cache=_int(
                environ, "CROSSTALK_TOPICS_LAYOUT_CACHE", default.layout_cache, 0, 1024
            ),
        )


def _int(environ: Mapping[str, str], name: str, default: int, low: int, high: int) -> int:
    match environ.get(name):
        case None:
            return default
        case text if text.isascii() and text.isdigit() and low <= int(text) <= high:
            return int(text)
        case text:
            raise ConfigError(name, text, f"an integer in {low}..={high}")


def _level(environ: Mapping[str, str], name: str, default: int) -> int:
    match environ.get(name):
        case None:
            return default
        case text if text.lower() in _LEVELS:
            return _LEVELS[text.lower()]
        case text:
            raise ConfigError(name, text, "one of " + ", ".join(_LEVELS))
