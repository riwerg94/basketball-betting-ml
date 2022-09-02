import logging
import os
import sys

from autologging import traced, logged
from dotenv import load_dotenv
from pathlib import Path
from string import Template
from pyhocon import ConfigFactory, HOCONConverter

DEFAULT_LOG_FORMAT   = (
    "%(asctime)s",
    ":%(levelname)s",
    ":%(process)s",
    ":%(name)s",
    ":%(funcName)s",
    ":%(message)s",
)

ROOT_CONFIG_PATH = str(Path("./settings.conf"))

def set_basic_logging(format_str: str = DEFAULT_LOG_FORMAT, log_warnings: bool = True, level: int = None):
    logging.captureWarnings(log_warnings)
    logging.basicConfig(
        level=level if level else int(os.getenv("LOGGING_LEVEL", logging.WARNING)),
        stream=sys.stdout,
        # format=format_str,
        datefmt="%Y-%m-%d %H-%M-%S",
    )

@traced
@logged
def get_config(path: str = ROOT_CONFIG_PATH):
    load_dotenv(str(Path(".env")), override=True)

    # allows for env variables in HOCON include directive
    config_expanded = Template(Path(path).read_text()).safe_substitute(**os.environ)

    CONFIG = ConfigFactory.parse_string(config_expanded)

    return CONFIG

if __name__ == "__main__":
    load_dotenv(override=True)
    if len((args := sys.argv[1:])):
        for arg in args:
            print(HOCONConverter.to_yaml(CONFIG.get(arg, f"Key not found {arg}")))
    else:
        print(HOCONConverter.to_yaml(CONFIG))