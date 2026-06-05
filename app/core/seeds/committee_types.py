"""Seed data for the committee_types reference table.

These codes are sourced from the Texas Ethics Commission (TEC) filerTypeCd
values.  As other states are added, extend this list with their codes — use
a state-prefixed code if the same acronym means something different elsewhere
(e.g. "TX_COH" vs "OK_COH").

Usage
-----
    from app.core.seeds.committee_types import COMMITTEE_TYPE_SEEDS
    from app.core.models.tables import CommitteeType

    with Session(engine) as session:
        for seed in COMMITTEE_TYPE_SEEDS:
            existing = session.get(CommitteeType, seed["code"])
            if not existing:
                session.add(CommitteeType(**seed))
        session.commit()
"""

from typing import TypedDict


class CommitteeTypeSeed(TypedDict):
    code: str
    full_title: str
    description: str


COMMITTEE_TYPE_SEEDS: list[CommitteeTypeSeed] = [
    # ── Texas Ethics Commission (TEC) filerTypeCd values ─────────────────────
    {
        "code": "COH",
        "full_title": "Candidate / Officeholder",
        "description": (
            "A candidate for or holder of a Texas state, district, county, or "
            "local office who is required to file campaign finance reports with "
            "the TEC.  The most common filer type."
        ),
    },
    {
        "code": "GPAC",
        "full_title": "General-Purpose Political Action Committee",
        "description": (
            "A political committee that accepts political contributions or makes "
            "political expenditures that are not specific to a single candidate "
            "or measure.  Includes traditional corporate and trade-association PACs."
        ),
    },
    {
        "code": "JCOH",
        "full_title": "Judicial Candidate / Officeholder",
        "description": (
            "A candidate for or holder of a Texas judicial office (district court "
            "judge, appellate judge, Supreme Court justice, etc.) who files under "
            "the Judicial Campaign Fairness Act (JCFA) contribution limits."
        ),
    },
    {
        "code": "SPAC",
        "full_title": "Specific-Purpose Political Action Committee",
        "description": (
            "A political committee that accepts contributions or makes expenditures "
            "for a single candidate, single officeholder, or single measure.  "
            "Must dissolve or convert once that purpose is complete."
        ),
    },
    {
        "code": "MPAC",
        "full_title": "Multicounty Political Action Committee",
        "description": (
            "A general-purpose political committee that operates across multiple "
            "Texas counties and files centrally with the TEC rather than with "
            "individual county clerks."
        ),
    },
    {
        "code": "PTYCORP",
        "full_title": "Political Party / Party Corporation",
        "description": (
            "A Texas state or county political party executive committee, or a "
            "political party corporation organized to accept corporate contributions "
            "for administrative and party-building activities."
        ),
    },
    {
        "code": "SCC",
        "full_title": "State/County Central Committee (Party)",
        "description": (
            "The state executive committee or a county executive committee of a "
            "Texas political party.  Files reports on money raised and spent for "
            "party-wide activities."
        ),
    },
    {
        "code": "DCE",
        "full_title": "Direct Campaign Expenditure",
        "description": (
            "A person or entity that makes direct campaign expenditures expressly "
            "advocating the election or defeat of a clearly identified candidate "
            "but does not coordinate with any campaign.  Filer is not a political "
            "committee."
        ),
    },
    {
        "code": "ASIFSPAC",
        "full_title": "As-If Specific-Purpose Committee",
        "description": (
            "A filer treated 'as if' it were a specific-purpose committee under "
            "TEC rules because its activity is effectively limited to supporting "
            "or opposing a single candidate or measure, even if not formally "
            "organised as a SPAC."
        ),
    },
    {
        "code": "CEC",
        "full_title": "County Executive Committee (Party)",
        "description": (
            "A county-level executive committee of a Texas political party that "
            "files campaign finance reports with its county clerk and, where "
            "required, with the TEC."
        ),
    },
    {
        "code": "JSPC",
        "full_title": "Judicial Specific-Purpose Committee",
        "description": (
            "A specific-purpose political committee formed to support or oppose a "
            "single judicial candidate.  Subject to the contribution limits of the "
            "Judicial Campaign Fairness Act (JCFA)."
        ),
    },
    {
        "code": "LEG",
        "full_title": "Legislative Caucus Committee",
        "description": (
            "A committee established by a legislative caucus of the Texas "
            "Legislature to finance caucus activities, including staff and "
            "administrative costs."
        ),
    },
    {
        "code": "SPK",
        "full_title": "Speaker Committee",
        "description": (
            "A committee established by a candidate for Speaker of the Texas "
            "House of Representatives to finance the campaign for that leadership "
            "position."
        ),
    },
    {
        "code": "SCPC",
        "full_title": "State/County Political Committee",
        "description": (
            "A political committee that operates at the state or county level and "
            "does not qualify as a general-purpose or specific-purpose PAC.  "
            "Relatively rare; used for certain hybrid or transitional filers."
        ),
    },
    {
        "code": "MCEC",
        "full_title": "Multicounty County Executive Committee",
        "description": (
            "A county executive committee of a Texas political party that spans "
            "multiple counties or files centrally due to the geographic scope of "
            "its activity."
        ),
    },
]
