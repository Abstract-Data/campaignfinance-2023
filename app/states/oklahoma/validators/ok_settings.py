"""
======================
==== OKEC Settings ===
======================
"""

from pydantic import ConfigDict, model_validator
from sqlmodel import SQLModel

import app.funcs.validator_functions as funcs


class OklahomaSettings(SQLModel):
    model_config = ConfigDict(
        populate_by_name=True,
        str_strip_whitespace=True,
        str_to_upper=True,
    )

    _clear_blank_strings = model_validator(mode="before")(funcs.clear_blank_strings)
