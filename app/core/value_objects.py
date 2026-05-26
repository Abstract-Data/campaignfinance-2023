"""Pure value objects for person, address, and officer field groups (RF-SMELL-004)."""

from __future__ import annotations

from dataclasses import dataclass


def _strip(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


@dataclass(frozen=True)
class PersonName:
    """Normalized person or organization name parts."""

    first: str | None = None
    middle: str | None = None
    last: str | None = None
    suffix: str | None = None
    organization: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "first", _strip(self.first))
        object.__setattr__(self, "middle", _strip(self.middle))
        object.__setattr__(self, "last", _strip(self.last))
        object.__setattr__(self, "suffix", _strip(self.suffix))
        object.__setattr__(self, "organization", _strip(self.organization))

    @property
    def full_name(self) -> str:
        if self.organization:
            return self.organization
        parts = [self.first, self.middle, self.last, self.suffix]
        return " ".join(part for part in parts if part)


@dataclass(frozen=True)
class AddressParts:
    """Normalized postal address components."""

    street_1: str | None = None
    street_2: str | None = None
    city: str | None = None
    state: str | None = None
    zip_code: str | None = None

    def normalized(self) -> AddressParts:
        state = _strip(self.state)
        return AddressParts(
            street_1=_strip(self.street_1),
            street_2=_strip(self.street_2),
            city=_strip(self.city),
            state=state.upper() if state else None,
            zip_code=_strip(self.zip_code),
        )


@dataclass(frozen=True)
class Officer:
    """Committee officer identity."""

    name: PersonName
    role: str
    committee_id: str
