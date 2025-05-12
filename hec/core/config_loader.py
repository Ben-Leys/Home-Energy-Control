import yaml
from dotenv import load_dotenv
from pathlib import Path

CONFIG_FILE_NAME = "config.yaml"
BASE_DIR = Path(__file__).resolve().parent.parent.parent  # Assumes config_loader.py is in core/


def load_app_config():
    """Loads application configuration from YAML and .env files."""
    # Load .env file
    env_path = BASE_DIR / "hec/.env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
    else:
        print(f"Warning: .env file not found at {env_path}")

    # Load YAML configuration
    config_path = BASE_DIR / "hec/" / CONFIG_FILE_NAME
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file '{CONFIG_FILE_NAME}' not found at {config_path}")

    with open(config_path, 'r') as f:
        try:
            config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML configuration file: {e}")

    return config
