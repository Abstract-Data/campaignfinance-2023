from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from app.abcs import StateConfig


class State(str, Enum):
    texas = "texas"


@dataclass(frozen=True)
class StateContext:
    config: StateConfig
    temp_folder: Path


def resolve_state(state: State, *, data_folder: Path | None = None) -> StateContext:
    if state is State.texas:
        from app.states.texas import TEXAS_CONFIGURATION

        temp_folder = (
            data_folder.expanduser().resolve()
            if data_folder is not None
            else TEXAS_CONFIGURATION.TEMP_FOLDER
        )
        return StateContext(
            config=TEXAS_CONFIGURATION,
            temp_folder=temp_folder,
        )
    msg = f"Unsupported state: {state.value}"
    raise ValueError(msg)
