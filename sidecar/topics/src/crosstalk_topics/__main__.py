"""`crosstalk-topics [serve]` runs the service; `crosstalk-topics
healthcheck` asks a local one for `/healthz` and exits 0 when it answers
200 (the container healthcheck; the image has no curl)."""

import logging
import os
import sys
import urllib.error
import urllib.request

from crosstalk_topics import log
from crosstalk_topics.config import Config, ConfigError
from crosstalk_topics.pins import PinError

logger = logging.getLogger("crosstalk_topics")


def serve(config: Config) -> int:
    import uvicorn

    from crosstalk_topics.app import create_app

    try:
        app = create_app(config)
    except PinError as error:
        log.event(logger, logging.ERROR, "refusing to start", reason=str(error))
        return 1
    log.event(logger, logging.INFO, "listening", host=config.host, port=config.port)
    uvicorn.run(app, host=config.host, port=config.port, log_config=None, access_log=False)
    return 0


def healthcheck(config: Config) -> int:
    url = f"http://127.0.0.1:{config.port}/healthz"
    try:
        with urllib.request.urlopen(url, timeout=3) as response:
            return 0 if response.status == 200 else 1
    except urllib.error.URLError, OSError:
        return 1


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    match args:
        case [] | ["serve"]:
            command = serve
        case ["healthcheck"]:
            command = healthcheck
        case _:
            print("usage: crosstalk-topics [serve | healthcheck]", file=sys.stderr)
            return 2
    try:
        config = Config.from_env(os.environ)
    except ConfigError as error:
        print(f"crosstalk-topics: {error}", file=sys.stderr)
        return 2
    if command is serve:
        log.configure(config.log_level)
    return command(config)


if __name__ == "__main__":
    sys.exit(main())
