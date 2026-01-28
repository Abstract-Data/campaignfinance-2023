#!/usr/bin/env python3
"""
Test script for committee-person relationships and versioning.
"""

from pathlib import Path
from icecream import ic
from datetime import date
from decimal import Decimal

from app.core.unified_sqlmodels import (
    unified_sql_processor, UnifiedTransaction, PersonRole, TransactionType,
    CommitteeRole
)
from app.core.unified_database import db_manager


def test_committee_person_relationships():
    """Test committee-person relationships and versioning."""
    
    ic("Testing Committee-Person Relationships")
    ic("=" * 50)
    
    # Test 1: Create sample data
    ic("\n1. Creating Sample Data")
    ic("-" * 30)
    
    # Create a person
    person_data = {
        "first_name": "John",
        "last_name": "Smith",
        "employer": "Smith & Associates",
        "occupation": "Attorney",
        "organization": None,
        "person_type": "individual"
    }
    
    # Create a committee
    committee_data = {
        "name": "Smith for Senate Committee",
        "committee_type": "candidate",
        "filer_id": "C12345"
    }
    
    # Save person and committee to database
    with db_manager.get_session() as session:
        from app.core.unified_sqlmodels import UnifiedPerson, UnifiedCommittee
        
        person = UnifiedPerson(**person_data)
        session.add(person)
        session.commit()
        session.refresh(person)
        
        committee = UnifiedCommittee(**committee_data)
        session.add(committee)
        session.commit()
        session.refresh(committee)
        
        ic(f"Created person: {person.full_name} (ID: {person.id})")
        ic(f"Created committee: {committee.name} (ID: {committee.id})")
    
    # Test 2: Add person to committee as treasurer
    ic("\n2. Adding Person to Committee as Treasurer")
    ic("-" * 30)
    
    treasurer_role = db_manager.add_person_to_committee(
        person_id=person.id,
        committee_id=committee.id,
        role=CommitteeRole.TREASURER,
        start_date=date(2024, 1, 1),
        notes="Primary treasurer for the campaign",
        user="admin_user"
    )
    
    ic(f"Added {person.full_name} as {treasurer_role.role} to {committee.name}")
    ic(f"Start date: {treasurer_role.start_date}")
    ic(f"Active: {treasurer_role.is_active}")
    
    # Test 3: Add same person as chair (multiple roles)
    ic("\n3. Adding Same Person as Chair (Multiple Roles)")
    ic("-" * 30)
    
    chair_role = db_manager.add_person_to_committee(
        person_id=person.id,
        committee_id=committee.id,
        role=CommitteeRole.CHAIR,
        start_date=date(2024, 1, 1),
        notes="Campaign chair and treasurer",
        user="admin_user"
    )
    
    ic(f"Added {person.full_name} as {chair_role.role} to {committee.name}")
    ic(f"Now serving as both {treasurer_role.role} and {chair_role.role}")
    
    # Test 4: Get all roles for the person
    ic("\n4. Getting All Roles for the Person")
    ic("-" * 30)
    
    person_roles = db_manager.get_person_committee_roles(person.id)
    ic(f"Roles for {person.full_name}:")
    for role in person_roles:
        ic(f"  - {role.role} at {committee.name} (since {role.start_date})")
    
    # Test 5: Get all people for the committee
    ic("\n5. Getting All People for the Committee")
    ic("-" * 30)
    
    committee_persons = db_manager.get_committee_persons(committee.id)
    ic(f"People in {committee.name}:")
    for cp in committee_persons:
        ic(f"  - {cp.person.full_name} as {cp.role}")
    
    # Test 6: Get committee officers grouped by role
    ic("\n6. Getting Committee Officers Grouped by Role")
    ic("-" * 30)
    
    officers = db_manager.get_committee_officers(committee.id)
    for role, people in officers.items():
        ic(f"{role.value}:")
        for cp in people:
            ic(f"  - {cp.person.full_name} (since {cp.start_date})")
    
    # Test 7: Update a role
    ic("\n7. Updating a Role")
    ic("-" * 30)
    
    updated_role = db_manager.update_committee_person(
        committee_person_id=treasurer_role.id,
        updates={"notes": "Updated: Primary treasurer with additional responsibilities"},
        user="admin_user",
        reason="Role expansion",
        amendment_details="Added additional treasurer responsibilities"
    )
    
    ic(f"Updated {updated_role.role} role:")
    ic(f"  Notes: {updated_role.notes}")
    ic(f"  Last modified: {updated_role.last_modified_at}")
    ic(f"  Modified by: {updated_role.last_modified_by}")
    
    # Test 8: Get version history
    ic("\n8. Getting Version History")
    ic("-" * 30)
    
    versions = db_manager.get_committee_person_versions(treasurer_role.id)
    ic(f"Version history for treasurer role:")
    for version in versions:
        ic(f"  Version {version.version_number}: {version.changed_at}")
        ic(f"    Changed by: {version.changed_by}")
        ic(f"    Reason: {version.change_reason}")
    
    # Test 9: Remove a role (set as inactive)
    ic("\n9. Removing a Role (Setting as Inactive)")
    ic("-" * 30)
    
    removed = db_manager.remove_person_from_committee(
        person_id=person.id,
        committee_id=committee.id,
        role=CommitteeRole.CHAIR,
        end_date=date(2024, 6, 30),
        user="admin_user",
        reason="Resigned as chair, continuing as treasurer"
    )
    
    ic(f"Removed chair role: {removed}")
    
    # Check active roles
    active_roles = db_manager.get_person_committee_roles(person.id, active_only=True)
    ic(f"Active roles for {person.full_name}:")
    for role in active_roles:
        ic(f"  - {role.role} at {committee.name}")
    
    # Test 10: Get all roles (including inactive)
    ic("\n10. Getting All Roles (Including Inactive)")
    ic("-" * 30)
    
    all_roles = db_manager.get_person_committee_roles(person.id, active_only=False)
    ic(f"All roles for {person.full_name}:")
    for role in all_roles:
        status = "Active" if role.is_active else "Inactive"
        end_date = f" until {role.end_date}" if role.end_date else ""
        ic(f"  - {role.role} at {committee.name} ({status}{end_date})")
    
    # Test 11: Add another person to the committee
    ic("\n11. Adding Another Person to the Committee")
    ic("-" * 30)
    
    # Create another person
    with db_manager.get_session() as session:
        person2 = UnifiedPerson(
            first_name="Jane",
            last_name="Doe",
            employer="Doe Consulting",
            occupation="Consultant",
            person_type="individual"
        )
        session.add(person2)
        session.commit()
        session.refresh(person2)
    
    # Add as assistant treasurer
    assistant_treasurer = db_manager.add_person_to_committee(
        person_id=person2.id,
        committee_id=committee.id,
        role=CommitteeRole.ASSISTANT_TREASURER,
        start_date=date(2024, 2, 1),
        notes="Assistant to the treasurer",
        user="admin_user"
    )
    
    ic(f"Added {person2.full_name} as {assistant_treasurer.role}")
    
    # Show all committee members
    all_members = db_manager.get_committee_persons(committee.id, active_only=True)
    ic(f"All active members of {committee.name}:")
    for member in all_members:
        ic(f"  - {member.person.full_name} as {member.role}")
    
    # Test 12: Get active treasurers
    ic("\n12. Getting Active Treasurers")
    ic("-" * 30)
    
    treasurers = db_manager.get_active_treasurers()
    ic(f"All active treasurers:")
    for treasurer in treasurers:
        ic(f"  - {treasurer.person.full_name} at {treasurer.committee.name}")
    
    # Test 13: Get treasurers for specific committee
    ic("\n13. Getting Treasurers for Specific Committee")
    ic("-" * 30)
    
    committee_treasurers = db_manager.get_active_treasurers(committee_id=committee.id)
    ic(f"Treasurers for {committee.name}:")
    for treasurer in committee_treasurers:
        ic(f"  - {treasurer.person.full_name} ({treasurer.role})")
    
    ic("\nCommittee-Person Relationship Test Complete!")
    ic("=" * 50)


if __name__ == "__main__":
    test_committee_person_relationships() 