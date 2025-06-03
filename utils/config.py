import yaml
from pathlib import Path

def load_etl_config(business: str, config_file: str = "etl_config.yml") -> dict:
    
    config_path = Path(f"src/{business}/config/{config_file}")
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found at: {config_path}")
    
    with open(config_path, "r") as file:
        return yaml.safe_load(file)
