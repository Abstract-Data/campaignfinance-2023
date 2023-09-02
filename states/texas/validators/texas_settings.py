from pydantic import BaseModel, ConfigDict

"""
======================
==== TEC Settings ====
======================
"""


class TECSettings(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        str_strip_whitespace=True,
        str_to_upper=True,
        from_attributes=True,
    )