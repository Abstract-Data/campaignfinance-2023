"""High-level unified SQLModel processor."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any

from sqlmodel import Session

from app.core.builders import UnifiedSQLModelBuilder
from app.core.enums import PersonRole, TransactionType
from app.core.models import (
    UnifiedAsset,
    UnifiedContribution,
    UnifiedCredit,
    UnifiedDebt,
    UnifiedExpenditure,
    UnifiedLoan,
    UnifiedPerson,
    UnifiedTransaction,
    UnifiedTransactionPerson,
    UnifiedTravel,
)

# Fix 1a: Dispatch table mapping TEC record_type → {PersonRole: field_prefix}.
# Only roles present in the dict are built for that record type.
# For record types not in this map the fallback is {PersonRole.CONTRIBUTOR: "contributor"}.
RECORD_TYPE_ROLE_MAP: dict[str, dict[PersonRole, str]] = {
    "RCPT":   {PersonRole.CONTRIBUTOR: "contributor"},   # contributions
    "EXPN":   {PersonRole.PAYEE: "payee"},               # expenditures
    "LOAN":   {PersonRole.CONTRIBUTOR: "lender"},        # loans (lender acts as contributor)
    "PLEDGE": {PersonRole.CONTRIBUTOR: "pledger"},       # pledges
    "CREDIT": {PersonRole.CONTRIBUTOR: "payor"},         # credits/refunds
    "TRAVEL": {PersonRole.PAYEE: "traveller"},           # travel (traveller acts as payee)
    "ASSET":  {},                                        # assets have no external person
}


@dataclass
class ProcessStats:
    """Per-batch processing counters for unified record ingestion."""

    success: int = 0
    failures: int = 0
    db_errors: int = 0
    skipped: int = 0

    @property
    def total(self) -> int:
        return self.success + self.failures + self.db_errors + self.skipped

    def __str__(self) -> str:
        return (
            f"Processed {self.total}: {self.success} OK, "
            f"{self.failures} validation failures, "
            f"{self.db_errors} DB errors, {self.skipped} skipped"
        )


DetailContext = dict[str, Any]
DetailBuilder = Callable[
    [UnifiedTransaction, UnifiedSQLModelBuilder, dict[str, Any], DetailContext], None
]


def _build_contribution_detail(
    transaction: UnifiedTransaction,
    builder: UnifiedSQLModelBuilder,
    raw_data: dict[str, Any],
    ctx: DetailContext,
) -> None:
    contributor_entity = ctx["contributor_entity"]
    recipient_entity = ctx["recipient_entity"]
    committee = ctx["committee"]
    recipient = ctx["recipient"]

    # Fix 2: never fall back to committee.entity as the contributor — that makes
    # the committee appear to donate to itself.  If the contributor is unknown,
    # skip creating the detail record entirely.
    if not contributor_entity:
        return  # anonymous/unknown contributor — skip UnifiedContribution
    if not recipient_entity:
        return  # no committee entity — skip UnifiedContribution

    transaction.contribution = UnifiedContribution(
        transaction=transaction,
        contributor=contributor_entity,
        recipient=recipient_entity,
        amount=transaction.amount,
        receipt_date=transaction.transaction_date,
        contribution_type=builder._get_field_value(raw_data, "contribution_type"),
        description=transaction.description,
        state_id=builder.state_id,
    )


def _build_loan_detail(
    transaction: UnifiedTransaction,
    builder: UnifiedSQLModelBuilder,
    raw_data: dict[str, Any],
    ctx: DetailContext,
) -> None:
    contributor_entity = ctx["contributor_entity"]
    recipient_entity = ctx["recipient_entity"]
    if not contributor_entity and recipient_entity:
        contributor_entity = recipient_entity
    if not (contributor_entity and recipient_entity):
        return

    transaction.loan = UnifiedLoan(
        transaction=transaction,
        lender=contributor_entity,
        borrower=recipient_entity,
        amount=transaction.amount,
        loan_date=transaction.transaction_date,
        due_date=builder._parse_date(builder._get_field_value(raw_data, "loan_due_date")),
        interest_rate=builder._parse_amount(
            builder._get_field_value(raw_data, "loan_interest_rate")
        ),
        collateral=builder._get_field_value(raw_data, "loan_collateral"),
        state_id=builder.state_id,
    )


def _build_debt_detail(
    transaction: UnifiedTransaction,
    builder: UnifiedSQLModelBuilder,
    raw_data: dict[str, Any],
    ctx: DetailContext,
) -> None:
    committee = ctx["committee"]
    creditor_entity = ctx["contributor_entity"]
    debtor_entity = ctx["recipient_entity"] or (
        committee.entity if committee and hasattr(committee, "entity") else None
    )
    if not creditor_entity:
        return

    transaction.debt = UnifiedDebt(
        transaction=transaction,
        creditor=creditor_entity,
        debtor=debtor_entity or creditor_entity,
        amount=transaction.amount,
        original_amount=builder._parse_amount(
            builder._get_field_value(raw_data, "debt_original_amount")
        )
        or transaction.amount,
        debt_date=transaction.transaction_date,
        due_date=builder._parse_date(builder._get_field_value(raw_data, "debt_due_date")),
        description=transaction.description,
        is_guaranteed=builder._parse_boolean(
            builder._get_field_value(raw_data, "loan_guaranteed_flag")
        ),
        guarantor_name=builder._get_field_value(raw_data, "guarantor_name"),
        guarantee_amount=builder._parse_amount(
            builder._get_field_value(raw_data, "loan_guarantee_amount")
        ),
        is_paid=builder._parse_boolean(builder._get_field_value(raw_data, "debt_paid_flag")),
        payment_amount=builder._parse_amount(
            builder._get_field_value(raw_data, "debt_payment_amount")
        ),
        payment_date=builder._parse_date(
            builder._get_field_value(raw_data, "debt_payment_date")
        ),
        state_id=builder.state_id,
    )


def _build_credit_detail(
    transaction: UnifiedTransaction,
    builder: UnifiedSQLModelBuilder,
    raw_data: dict[str, Any],
    ctx: DetailContext,
) -> None:
    committee = ctx["committee"]
    payor_entity = ctx["contributor_entity"]
    recipient_ent = ctx["recipient_entity"] or (
        committee.entity if committee and hasattr(committee, "entity") else None
    )
    if not payor_entity:
        return

    transaction.credit = UnifiedCredit(
        transaction=transaction,
        payor=payor_entity,
        recipient=recipient_ent or payor_entity,
        amount=transaction.amount,
        credit_date=transaction.transaction_date,
        credit_type=builder._get_field_value(raw_data, "credit_type"),
        description=transaction.description,
        related_transaction_id=builder._get_field_value(raw_data, "related_transaction_id"),
        state_id=builder.state_id,
    )


def _build_travel_detail(
    transaction: UnifiedTransaction,
    builder: UnifiedSQLModelBuilder,
    raw_data: dict[str, Any],
    ctx: DetailContext,
) -> None:
    contributor = ctx["contributor"]
    traveler_name = builder._get_field_value(raw_data, "traveler_name") or builder._get_field_value(
        raw_data, "parent_full_name"
    )

    transaction.travel = UnifiedTravel(
        transaction=transaction,
        traveler=contributor if contributor else None,
        state_id=builder.state_id,
        parent_transaction_type=builder._get_field_value(raw_data, "parent_type"),
        parent_transaction_id=builder._get_field_value(raw_data, "parent_id"),
        parent_amount=builder._parse_amount(builder._get_field_value(raw_data, "parent_amount")),
        amount=transaction.amount,
        travel_date=transaction.transaction_date,
        transportation_type=builder._get_field_value(raw_data, "transportation_type_cd")
        or builder._get_field_value(raw_data, "transportation_type"),
        transportation_description=builder._get_field_value(
            raw_data, "transportation_type_descr"
        ),
        departure_city=builder._get_field_value(raw_data, "departure_city"),
        departure_state=builder._get_field_value(raw_data, "departure_state"),
        arrival_city=builder._get_field_value(raw_data, "arrival_city"),
        arrival_state=builder._get_field_value(raw_data, "arrival_state"),
        departure_date=builder._parse_date(builder._get_field_value(raw_data, "departure_dt")),
        arrival_date=builder._parse_date(builder._get_field_value(raw_data, "arrival_dt")),
        travel_purpose=builder._get_field_value(raw_data, "travel_purpose")
        or transaction.description,
        traveler_name=traveler_name,
    )


def _build_asset_detail(
    transaction: UnifiedTransaction,
    builder: UnifiedSQLModelBuilder,
    raw_data: dict[str, Any],
    ctx: DetailContext,
) -> None:
    committee = ctx["committee"]
    transaction.asset = UnifiedAsset(
        transaction=transaction,
        committee=committee,
        state_id=builder.state_id,
        asset_type=builder._get_field_value(raw_data, "asset_type"),
        description=transaction.description or builder._get_field_value(raw_data, "asset_descr"),
        acquisition_date=transaction.transaction_date,
        acquisition_cost=transaction.amount,
        current_value=builder._parse_amount(
            builder._get_field_value(raw_data, "asset_current_value")
        ),
        valuation_date=builder._parse_date(
            builder._get_field_value(raw_data, "asset_valuation_date")
        ),
        disposition_date=builder._parse_date(
            builder._get_field_value(raw_data, "asset_disposition_date")
        ),
        disposition_amount=builder._parse_amount(
            builder._get_field_value(raw_data, "asset_disposition_amount")
        ),
        is_disposed=builder._parse_boolean(
            builder._get_field_value(raw_data, "asset_disposed_flag")
        ),
    )


def _build_expenditure_detail(
    transaction: UnifiedTransaction,
    builder: UnifiedSQLModelBuilder,
    raw_data: dict[str, Any],
    ctx: DetailContext,
) -> None:
    """Create a UnifiedExpenditure for EXPN transactions (Fix 3b).

    After Fix 1 the payee person is stored in the PAYEE slot.  The committee
    is always the payer.  ``ctx["payee_entity"]`` carries the payee entity
    resolved by ``_entity_context``; ``ctx["committee"]`` carries the
    committee whose entity is the payer.
    """
    committee = ctx["committee"]
    payee_entity = ctx["payee_entity"]
    payer_entity = committee.entity if committee and committee.entity else None
    if not (payer_entity and payee_entity):
        return
    transaction.expenditure = UnifiedExpenditure(
        transaction=transaction,
        payer=payer_entity,
        payee=payee_entity,
        amount=transaction.amount,
        expenditure_date=transaction.transaction_date,
        expenditure_type=builder._get_field_value(raw_data, "expenditure_type"),
        description=transaction.description,
        state_id=builder.state_id,
    )


DETAIL_BUILDERS: dict[TransactionType, DetailBuilder] = {
    TransactionType.CONTRIBUTION: _build_contribution_detail,
    TransactionType.EXPENDITURE: _build_expenditure_detail,
    TransactionType.LOAN: _build_loan_detail,
    TransactionType.DEBT: _build_debt_detail,
    TransactionType.CREDIT: _build_credit_detail,
    TransactionType.TRAVEL: _build_travel_detail,
    TransactionType.ASSET: _build_asset_detail,
}


def _build_participants(
    builder: UnifiedSQLModelBuilder, raw_data: dict[str, Any]
) -> dict[PersonRole, UnifiedPerson | None]:
    # Fix 1d: Use RECORD_TYPE_ROLE_MAP so only the correct role is built for
    # each TEC record type, with the matching field prefix.  All other roles
    # default to None, eliminating phantom RECIPIENT/PAYEE/CANDIDATE rows.
    record_type = raw_data.get("record_type", "").upper()
    role_map = RECORD_TYPE_ROLE_MAP.get(
        record_type, {PersonRole.CONTRIBUTOR: "contributor"}
    )
    result: dict[PersonRole, UnifiedPerson | None] = {role: None for role in PersonRole}
    for role, prefix in role_map.items():
        result[role] = builder.build_person(raw_data, role, field_prefix=prefix)
    return result


def _attach_transaction_persons(
    transaction: UnifiedTransaction,
    builder: UnifiedSQLModelBuilder,
    participants: dict[PersonRole, UnifiedPerson | None],
) -> None:
    for role, person in participants.items():
        if not person:
            continue
        # Fix 6: RECIPIENT is always the committee, captured via committee_id FK.
        # Never write a phantom RECIPIENT row into unified_transaction_persons.
        if role == PersonRole.RECIPIENT:
            continue
        transaction.persons.append(
            UnifiedTransactionPerson(
                transaction=transaction,
                person=person,
                entity=person.entity,
                state_id=builder.state_id,
                role=role,
            )
        )


def _entity_context(
    participants: dict[PersonRole, UnifiedPerson | None],
    committee: Any,
) -> DetailContext:
    contributor = participants[PersonRole.CONTRIBUTOR]
    recipient = participants[PersonRole.RECIPIENT]
    payee = participants[PersonRole.PAYEE]
    contributor_entity = contributor.entity if contributor and contributor.entity else None
    payee_entity = payee.entity if payee and payee.entity else None
    recipient_entity = None
    if committee and committee.entity:
        recipient_entity = committee.entity
    elif recipient and recipient.entity:
        recipient_entity = recipient.entity
    return {
        "contributor": contributor,
        "recipient": recipient,
        "payee": payee,
        "committee": committee,
        "contributor_entity": contributor_entity,
        "payee_entity": payee_entity,
        "recipient_entity": recipient_entity,
    }


def _attach_detail_record(
    transaction: UnifiedTransaction,
    builder: UnifiedSQLModelBuilder,
    raw_data: dict[str, Any],
    ctx: DetailContext,
) -> None:
    detail_builder = DETAIL_BUILDERS.get(transaction.transaction_type)
    if detail_builder is not None:
        detail_builder(transaction, builder, raw_data, ctx)


class UnifiedSQLDataProcessor:
    """High-level processor for converting state-specific data to SQLModel instances."""

    def get_builder(
        self,
        state: str,
        state_id: int | None = None,
        state_code: str | None = None,
        *,
        session: Session | None = None,
    ) -> UnifiedSQLModelBuilder:
        """Return a fresh builder per call (no shared mutable cache)."""
        return UnifiedSQLModelBuilder(
            state,
            state_id,
            state_code,
            session=session,
        )

    def process_record(
        self,
        raw_data: dict[str, Any],
        state: str,
        state_id: int | None = None,
        state_code: str | None = None,
        *,
        session: Session | None = None,
    ) -> UnifiedTransaction:
        """Process a single record from any state into a unified transaction."""
        builder = self.get_builder(
            state, state_id=state_id, state_code=state_code, session=session
        )
        participants = _build_participants(builder, raw_data)
        committee = builder.build_committee(raw_data)
        transaction = builder.build_transaction(raw_data)
        candidate = participants[PersonRole.CANDIDATE]

        campaign = builder.build_campaign(raw_data, committee, candidate, transaction)
        if campaign:
            transaction.campaign = campaign

        if committee:
            transaction.committee_id = committee.filer_id
            transaction.committee = committee

        _attach_transaction_persons(transaction, builder, participants)
        ctx = _entity_context(participants, committee)
        _attach_detail_record(transaction, builder, raw_data, ctx)
        return transaction

    def process_record_stream(
        self,
        records: Iterator[dict[str, Any]] | list[dict[str, Any]],
        state: str,
        state_id: int | None = None,
        state_code: str | None = None,
        *,
        session: Session | None = None,
    ) -> Iterator[UnifiedTransaction]:
        """Yield unified transactions one record at a time (P2-PERF-002)."""
        for record in records:
            yield self.process_record(
                record,
                state,
                state_id=state_id,
                state_code=state_code,
                session=session,
            )

    def process_records(
        self,
        records: list[dict[str, Any]],
        state: str,
        state_id: int | None = None,
        state_code: str | None = None,
        *,
        session: Session | None = None,
    ) -> list[UnifiedTransaction]:
        """Process multiple records; thin wrapper over ``process_record_stream``."""
        return list(
            self.process_record_stream(
                records,
                state,
                state_id=state_id,
                state_code=state_code,
                session=session,
            )
        )


# Global processor instance
unified_sql_processor = UnifiedSQLDataProcessor()


from app.core.source_models.reports import UnifiedReport as _UnifiedReport  # noqa: E402,F401
