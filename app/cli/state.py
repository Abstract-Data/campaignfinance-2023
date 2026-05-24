from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from abcs import StateConfig


class State(str, Enum):
    texas = "texas"


@dataclass(frozen=True)
class StateContext:
    config: StateConfig
    temp_folder: Path


def resolve_state(state: State) -> StateContext:
    if state is State.texas:
        from app.states.texas import TEXAS_CONFIGURATION

        return StateContext(
            config=TEXAS_CONFIGURATION,
            temp_folder=TEXAS_CONFIGURATION.TEMP_FOLDER,
        )
    msg = f"Unsupported state: {state.value}"
    raise ValueError(msg)
