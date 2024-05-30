import tomli
from pathlib import Path

root = Path(__file__).parent / 'texas' / 'texas_fields.toml'
config = tomli.load(open(root, 'rb'))
