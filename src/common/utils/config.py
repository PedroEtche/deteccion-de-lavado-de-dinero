import yaml
import logging
from typing import Dict

def load_yaml_config(file_path: str = "./config.yaml") -> Dict:
    """Loads a YAML configuration file safely."""
    try:
        with open(file_path, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        logging.warning("Config file not found at %s. Using default/env values.", file_path)
        return {}
    except yaml.YAMLError as e:
        logging.error("Malformed YAML in config file %s: %s", file_path, e)
        return {}