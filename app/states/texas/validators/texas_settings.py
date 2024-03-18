from pydantic import ConfigDict
from sqlmodel import SQLModel

"""
======================
==== TEC Settings ====
======================
"""


class TECSettings(SQLModel):
    model_config = ConfigDict(
        populate_by_name=True,
        str_strip_whitespace=True,
        str_to_upper=True,
        from_attributes=True,
    )
