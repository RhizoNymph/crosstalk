import logging

import pytest

from crosstalk_topics.config import Config, ConfigError


def test_defaults() -> None:
    config = Config.from_env({})
    assert config == Config(
        host="0.0.0.0", port=8090, log_level=logging.INFO, max_body_bytes=1 << 30, layout_cache=4
    )


def test_values_are_read() -> None:
    config = Config.from_env(
        {
            "CROSSTALK_TOPICS_HOST": "127.0.0.1",
            "CROSSTALK_TOPICS_PORT": "9000",
            "CROSSTALK_TOPICS_LOG_LEVEL": "DEBUG",
            "CROSSTALK_TOPICS_MAX_BODY_BYTES": "1024",
            "CROSSTALK_TOPICS_LAYOUT_CACHE": "0",
        }
    )
    assert config == Config("127.0.0.1", 9000, logging.DEBUG, 1024, 0)


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("CROSSTALK_TOPICS_PORT", "0"),
        ("CROSSTALK_TOPICS_PORT", "eighty"),
        ("CROSSTALK_TOPICS_PORT", "-1"),
        ("CROSSTALK_TOPICS_LOG_LEVEL", "loud"),
        ("CROSSTALK_TOPICS_MAX_BODY_BYTES", "0"),
    ],
)
def test_bad_values_are_refused(name: str, value: str) -> None:
    with pytest.raises(ConfigError) as error:
        Config.from_env({name: value})
    assert error.value.variable == name
