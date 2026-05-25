"""High-level unified SQLModel processor."""

from __future__ import annotations

from typing import Any

from sqlalchemy.orm import selectinload
from sqlmodel import Session, select

from app.core.builders import UnifiedSQLModelBuilder
from app.core.enums import PersonRole, TransactionType
from app.core.models import (
    UnifiedAsset,
    UnifiedContribution,
    UnifiedCredit,
    UnifiedDebt,
    UnifiedLoan,
    UnifiedTransaction,
    UnifiedTransactionPerson,
    UnifiedTravel,
)

class UnifiedSQLDataProcessor:
    """
    High-level processor for converting state-specific data to SQLModel instances.
    """

    def __init__(self):
        self.builders: dict[str, UnifiedSQLModelBuilder] = {}

    def get_builder(
        self,
        state: str,
        state_id: int | None = None,
        state_code: str | None = None,
        *,
        session: Session | None = None,
    ) -> UnifiedSQLModelBuilder:
        """Get or create a model builder for a specific state.

        The ``session`` (when supplied) is forwarded to the cached builder so
        subsequent ``_find_*`` lookups dedupe against the caller's transaction
        (RF-SMELL-005).  Passing ``session=None`` leaves the existing session
        on the cached builder untouched.
        """
        if state not in self.builders:
            self.builders[state] = UnifiedSQLModelBuilder(
                state, state_id, state_code, session=session
            )
        builder = self.builders[state]
        builder.state_id = state_id
        builder.state_code = state_code
        if session is not None:
            builder.session = session
        return builder

    def process_record(
        self,
        raw_data: dict[str, Any],
        state: str,
        state_id: int | None = None,
        state_code: str | None = None,
        *,
        session: Session | None = None,
    ) -> UnifiedTransaction:
        """
        Process a single record from any state into a unified transaction.

        Args:
            raw_data: Raw data dictionary from the state
            state: State identifier (e.g., 'texas', 'oklahoma')

        Returns:
            UnifiedTransaction object
        """
        builder = self.get_builder(state, state_id=state_id, state_code=state_code, session=session)

        # Build related entities first (committee, persons, addresses)
        contributor = builder.build_person(raw_data, PersonRole.CONTRIBUTOR)
        recipient = builder.build_person(raw_data, PersonRole.RECIPIENT)
        payee = builder.build_person(raw_data, PersonRole.PAYEE)
        candidate = builder.build_person(raw_data, PersonRole.CANDIDATE)
        committee = builder.build_committee(raw_data)

        # Build the transaction
        transaction = builder.build_transaction(raw_data)

        # Build campaign if possible
        campaign = builder.build_campaign(raw_data, committee, candidate, transaction)
        if campaign:
            transaction.campaign = campaign

        # Set committee relationship
        if committee:
            transaction.committee_id = committee.filer_id
            transaction.committee = committee

        # Create transaction-person relationships
        if contributor:
            tx_person = UnifiedTransactionPerson(
                transaction=transaction,
                person=contributor,
                entity=contributor.entity,
                state_id=builder.state_id,
                role=PersonRole.CONTRIBUTOR,
            )
            transaction.persons.append(tx_person)

        if recipient:
            tx_person = UnifiedTransactionPerson(
                transaction=transaction,
                person=recipient,
                entity=recipient.entity,
                state_id=builder.state_id,
                role=PersonRole.RECIPIENT,
            )
            transaction.persons.append(tx_person)

        if payee:
            tx_person = UnifiedTransactionPerson(
                transaction=transaction,
                person=payee,
                entity=payee.entity,
                state_id=builder.state_id,
                role=PersonRole.PAYEE,
            )
            transaction.persons.append(tx_person)

        if candidate:
            tx_person = UnifiedTransactionPerson(
                transaction=transaction,
                person=candidate,
                entity=candidate.entity,
                state_id=builder.state_id,
                role=PersonRole.CANDIDATE,
            )
            transaction.persons.append(tx_person)

        # Create specialized financial records
        contributor_entity = contributor.entity if contributor and contributor.entity else None
        recipient_entity = None
        if committee and committee.entity:
            recipient_entity = committee.entity
        elif recipient and recipient.entity:
            recipient_entity = recipient.entity

        if transaction.transaction_type == TransactionType.CONTRIBUTION:
            if not contributor_entity and committee and committee.entity:
                contributor_entity = committee.entity
            if not recipient_entity and recipient and recipient.entity:
                recipient_entity = recipient.entity
            if contributor_entity and recipient_entity:
                contribution = UnifiedContribution(
                    transaction=transaction,
                    contributor=contributor_entity,
                    recipient=recipient_entity,
                    amount=transaction.amount,
                    receipt_date=transaction.transaction_date,
                    contribution_type=builder._get_field_value(raw_data, "contribution_type"),
                    description=transaction.description,
                    state_id=builder.state_id,
                )
                transaction.contribution = contribution

        if transaction.transaction_type == TransactionType.LOAN:
            if not contributor_entity and recipient_entity:
                contributor_entity = recipient_entity
            if contributor_entity and recipient_entity:
                loan = UnifiedLoan(
                    transaction=transaction,
                    lender=contributor_entity,
                    borrower=recipient_entity,
                    amount=transaction.amount,
                    loan_date=transaction.transaction_date,
                    due_date=builder._parse_date(
                        builder._get_field_value(raw_data, "loan_due_date")
                    ),
                    interest_rate=builder._parse_amount(
                        builder._get_field_value(raw_data, "loan_interest_rate")
                    ),
                    collateral=builder._get_field_value(raw_data, "loan_collateral"),
                    state_id=builder.state_id,
                )
                transaction.loan = loan

        # Create debt detail record
        if transaction.transaction_type == TransactionType.DEBT:
            # For debts, the contributor is the creditor (who is owed money)
            # and the committee/campaign is the debtor
            creditor_entity = contributor_entity
            debtor_entity = recipient_entity or (
                committee.entity if committee and hasattr(committee, "entity") else None
            )

            if creditor_entity:
                debt = UnifiedDebt(
                    transaction=transaction,
                    creditor=creditor_entity,
                    debtor=debtor_entity or creditor_entity,  # Fallback if no debtor
                    amount=transaction.amount,
                    original_amount=builder._parse_amount(
                        builder._get_field_value(raw_data, "debt_original_amount")
                    )
                    or transaction.amount,
                    debt_date=transaction.transaction_date,
                    due_date=builder._parse_date(
                        builder._get_field_value(raw_data, "debt_due_date")
                    ),
                    description=transaction.description,
                    is_guaranteed=builder._parse_boolean(
                        builder._get_field_value(raw_data, "loan_guaranteed_flag")
                    ),
                    guarantor_name=builder._get_field_value(raw_data, "guarantor_name"),
                    guarantee_amount=builder._parse_amount(
                        builder._get_field_value(raw_data, "loan_guarantee_amount")
                    ),
                    is_paid=builder._parse_boolean(
                        builder._get_field_value(raw_data, "debt_paid_flag")
                    ),
                    payment_amount=builder._parse_amount(
                        builder._get_field_value(raw_data, "debt_payment_amount")
                    ),
                    payment_date=builder._parse_date(
                        builder._get_field_value(raw_data, "debt_payment_date")
                    ),
                    state_id=builder.state_id,
                )
                transaction.debt = debt

        # Create credit detail record
        if transaction.transaction_type == TransactionType.CREDIT:
            payor_entity = contributor_entity  # Who is giving the credit/refund
            recipient_ent = recipient_entity or (
                committee.entity if committee and hasattr(committee, "entity") else None
            )

            if payor_entity:
                credit = UnifiedCredit(
                    transaction=transaction,
                    payor=payor_entity,
                    recipient=recipient_ent or payor_entity,  # Fallback
                    amount=transaction.amount,
                    credit_date=transaction.transaction_date,
                    credit_type=builder._get_field_value(raw_data, "credit_type"),
                    description=transaction.description,
                    related_transaction_id=builder._get_field_value(
                        raw_data, "related_transaction_id"
                    ),
                    state_id=builder.state_id,
                )
                transaction.credit = credit

        # Create travel detail record
        if transaction.transaction_type == TransactionType.TRAVEL:
            # Get traveler info
            traveler_name = builder._get_field_value(
                raw_data, "traveler_name"
            ) or builder._get_field_value(raw_data, "parent_full_name")

            travel = UnifiedTravel(
                transaction=transaction,
                traveler=contributor if contributor else None,
                state_id=builder.state_id,
                # Parent transaction info
                parent_transaction_type=builder._get_field_value(raw_data, "parent_type"),
                parent_transaction_id=builder._get_field_value(raw_data, "parent_id"),
                parent_amount=builder._parse_amount(
                    builder._get_field_value(raw_data, "parent_amount")
                ),
                # Travel details
                amount=transaction.amount,
                travel_date=transaction.transaction_date,
                transportation_type=builder._get_field_value(raw_data, "transportation_type_cd")
                or builder._get_field_value(raw_data, "transportation_type"),
                transportation_description=builder._get_field_value(
                    raw_data, "transportation_type_descr"
                ),
                # Itinerary
                departure_city=builder._get_field_value(raw_data, "departure_city"),
                departure_state=builder._get_field_value(raw_data, "departure_state"),
                arrival_city=builder._get_field_value(raw_data, "arrival_city"),
                arrival_state=builder._get_field_value(raw_data, "arrival_state"),
                departure_date=builder._parse_date(
                    builder._get_field_value(raw_data, "departure_dt")
                ),
                arrival_date=builder._parse_date(builder._get_field_value(raw_data, "arrival_dt")),
                # Purpose
                travel_purpose=builder._get_field_value(raw_data, "travel_purpose")
                or transaction.description,
                traveler_name=traveler_name,
            )
            transaction.travel = travel

        # Create asset detail record
        if transaction.transaction_type == TransactionType.ASSET:
            asset = UnifiedAsset(
                transaction=transaction,
                committee=committee,
                state_id=builder.state_id,
                # Asset details
                asset_type=builder._get_field_value(raw_data, "asset_type"),
                description=transaction.description
                or builder._get_field_value(raw_data, "asset_descr"),
                # Valuation
                acquisition_date=transaction.transaction_date,
                acquisition_cost=transaction.amount,
                current_value=builder._parse_amount(
                    builder._get_field_value(raw_data, "asset_current_value")
                ),
                valuation_date=builder._parse_date(
                    builder._get_field_value(raw_data, "asset_valuation_date")
                ),
                # Disposition
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
            transaction.asset = asset

        return transaction

    def process_records(
        self,
        records: List[dict[str, Any]],
        state: str,
        state_id: int | None = None,
        state_code: str | None = None,
        *,
        session: Session | None = None,
    ) -> list[UnifiedTransaction]:
        """
        Process multiple records from any state into unified transactions.

        Args:
            records: List of raw data dictionaries
            state: State identifier
            session: Optional SQLAlchemy session for builder lookups
                (RF-SMELL-005).  When omitted, the builder's ``_find_*``
                helpers short-circuit to ``None``.

        Returns:
            List of UnifiedTransaction objects
        """
        return [
            self.process_record(
                record, state, state_id=state_id, state_code=state_code, session=session
            )
            for record in records
        ]


# Global processor instance
unified_sql_processor = UnifiedSQLDataProcessor()


# Resolve forward-string relationship references at module load time so the
# SQLAlchemy mapper registry can locate models declared in sibling modules
# (e.g. ``UnifiedReport`` lives in ``app.core.source_models.reports``).
# Without this import the mapper raises InvalidRequestError when any
# unified model is instantiated outside the loader pipeline (e.g. in tests).
from app.core.source_models.reports import UnifiedReport as _UnifiedReport  # noqa: E402,F401
