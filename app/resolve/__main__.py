"""Allow ``python -m app.resolve`` to invoke the resolve CLI."""

import sys

from app.resolve.cli import main

sys.exit(main())
