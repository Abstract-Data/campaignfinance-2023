from pydantic import BaseModel, Field, ConfigDict, model_validator, computed_field, HttpUrl, field_validator
from typing import Optional, Type, Dict, Union, Annotated
import sqlmodel
import sqlalchemy
from pathlib import Path


class CSVReaderConfig(BaseModel):
    lowercase_headers: bool = False
    replace_space_in_headers: bool = False


class CategoryConfig(BaseModel):
    PREFIX: Optional[str] = None
    SUFFIX: Optional[str] = None
    VALIDATOR: Type[sqlmodel.SQLModel]

    @model_validator(mode='after')
    def check_if_prefix_or_suffix(self):
        if not self.PREFIX and not self.SUFFIX:
            raise ValueError("Either PREFIX or SUFFIX must be defined.")
        return self


# class StateDownloadConfig(BaseModel):
#     ZIPFILE_URL: HttpUrl
#     TEMP_FILENAME: Path


class StateConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    STATE_NAME: str
    STATE_ABBREVIATION: Annotated[str, Field(max_length=2)]
    CATEGORY_TYPES: Dict[str, CategoryConfig]
    CSV_CONFIG: CSVReaderConfig

    @property
    def TEMP_FOLDER(self) -> Path:
        return Path(__file__).parents[2] / "tmp" / self.STATE_NAME.lower()

    # @model_validator(mode='after')
    # def set_download_filename(self):
    #     if self.DOWNLOAD_CONFIG:
    #         self.DOWNLOAD_CONFIG.TEMP_FILENAME = self.TEMP_FOLDER.joinpath(self.STATE_NAME.lower() + "_cf.zip")
    #     return self
