#!/usr/bin/env python3
"""
Script to analyze campaign finance fields and populate the unified field library.
"""

from pathlib import Path
from icecream import ic
from app.states.field_analyzer import analyze_campaign_finance_fields
from app.states.unified_field_library import field_library


def main():
    """Main function to analyze fields and populate the library."""
    
    ic("Starting campaign finance field analysis...")
    
    # Run the field analysis
    analyzer = analyze_campaign_finance_fields()
    
    # Get summary
    summary = analyzer.generate_field_summary()
    
    ic("\n=== FIELD ANALYSIS SUMMARY ===")
    ic(f"Total states analyzed: {summary['total_states']}")
    ic(f"Total unique fields found: {summary['total_unique_fields']}")
    ic(f"Unmapped fields: {len(summary['unmapped_fields'])}")
    
    # Show state breakdown
    for state, state_summary in summary['states'].items():
        ic(f"\n{state.upper()}:")
        ic(f"  Files analyzed: {state_summary['total_files']}")
        ic(f"  Unique fields: {state_summary['unique_fields']}")
        ic(f"  Mapped fields: {state_summary['mapped_fields']}")
        ic(f"  Unmapped fields: {len(state_summary['unmapped_fields'])}")
        
        # Show top unmapped fields
        if state_summary['unmapped_fields']:
            ic(f"  Top unmapped fields:")
            for field in state_summary['unmapped_fields'][:5]:
                ic(f"    - {field}")
    
    # Show field categories
    ic(f"\n=== FIELD CATEGORIES ===")
    for category, fields in summary['field_categories'].items():
        if fields:
            ic(f"{category}: {len(fields)} fields")
            for field in fields[:3]:  # Show first 3 examples
                ic(f"  - {field['field']} ({field['state']})")
    
    # Generate suggestions for unmapped fields
    ic(f"\n=== FIELD MAPPING SUGGESTIONS ===")
    for state in summary['states'].keys():
        suggestions = analyzer.suggest_field_mappings(state, min_confidence=0.6)
        if suggestions:
            ic(f"\n{state.upper()} suggestions:")
            for suggestion in suggestions[:10]:  # Show top 10
                ic(f"  {suggestion['state_field']} -> {suggestion['unified_field']} (confidence: {suggestion['confidence']:.2f})")
    
    # Export the current field library
    output_path = Path("unified_field_library.json")
    field_library.export_mappings(output_path)
    ic(f"\nField library exported to {output_path}")
    
    ic("\nAnalysis complete! Check the generated files for detailed results.")


if __name__ == "__main__":
    main() 