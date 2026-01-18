"""
Field Analyzer for Campaign Finance Data

This utility helps analyze field patterns across different states and data files,
and assists in populating the unified field library.
"""

from typing import Dict, List, Set, Optional, Any
from pathlib import Path
import polars as pl
from collections import defaultdict, Counter
import re
from icecream import ic

from .unified_field_library import UnifiedFieldLibrary, FieldCategory, FieldType, FieldDefinition, StateFieldMapping


class FieldAnalyzer:
    """
    Analyzes field patterns across campaign finance data files
    and helps populate the unified field library.
    """
    
    def __init__(self, field_library: UnifiedFieldLibrary):
        self.field_library = field_library
        self.field_patterns: Dict[str, Dict[str, Any]] = defaultdict(dict)
        self.field_frequencies: Dict[str, Counter] = defaultdict(Counter)
        self.suggested_mappings: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    
    def analyze_parquet_file(self, file_path: Path, state: str = "unknown") -> Dict[str, Any]:
        """
        Analyze a single Parquet file to understand its field structure.
        
        Args:
            file_path: Path to the Parquet file
            state: State identifier for the file
            
        Returns:
            Dictionary containing analysis results
        """
        try:
            # Read the Parquet file
            df = pl.read_parquet(file_path)
            
            analysis = {
                "file_path": str(file_path),
                "state": state,
                "total_rows": len(df),
                "total_columns": len(df.columns),
                "columns": list(df.columns),
                "column_types": {col: str(dtype) for col, dtype in df.schema.items()},
                "sample_values": {},
                "null_counts": {},
                "unique_counts": {}
            }
            
            # Analyze each column
            for col in df.columns:
                # Get sample values (first non-null value)
                sample_value = df.select(pl.col(col)).filter(pl.col(col).is_not_null()).item(0, 0) if len(df) > 0 else None
                analysis["sample_values"][col] = str(sample_value)[:100] if sample_value is not None else None
                
                # Count nulls
                null_count = df.select(pl.col(col).is_null().sum()).item(0, 0)
                analysis["null_counts"][col] = null_count
                
                # Count unique values
                unique_count = df.select(pl.col(col).n_unique()).item(0, 0)
                analysis["unique_counts"][col] = unique_count
            
            # Update field frequencies
            for col in df.columns:
                self.field_frequencies[state][col] += 1
            
            return analysis
            
        except Exception as e:
            ic(f"Error analyzing {file_path}: {e}")
            return {"file_path": str(file_path), "error": str(e)}
    
    def analyze_csv_file(self, file_path: Path, state: str = "unknown") -> Dict[str, Any]:
        """
        Analyze a single CSV file to understand its field structure.
        
        Args:
            file_path: Path to the CSV file
            state: State identifier for the file
            
        Returns:
            Dictionary containing analysis results
        """
        try:
            # Read the CSV file
            df = pl.read_csv(file_path)
            
            analysis = {
                "file_path": str(file_path),
                "state": state,
                "total_rows": len(df),
                "total_columns": len(df.columns),
                "columns": list(df.columns),
                "column_types": {col: str(dtype) for col, dtype in df.schema.items()},
                "sample_values": {},
                "null_counts": {},
                "unique_counts": {}
            }
            
            # Analyze each column
            for col in df.columns:
                # Get sample values (first non-null value)
                sample_value = df.select(pl.col(col)).filter(pl.col(col).is_not_null()).item(0, 0) if len(df) > 0 else None
                analysis["sample_values"][col] = str(sample_value)[:100] if sample_value is not None else None
                
                # Count nulls
                null_count = df.select(pl.col(col).is_null().sum()).item(0, 0)
                analysis["null_counts"][col] = null_count
                
                # Count unique values
                unique_count = df.select(pl.col(col).n_unique()).item(0, 0)
                analysis["unique_counts"][col] = unique_count
            
            # Update field frequencies
            for col in df.columns:
                self.field_frequencies[state][col] += 1
            
            return analysis
            
        except Exception as e:
            ic(f"Error analyzing {file_path}: {e}")
            return {"file_path": str(file_path), "error": str(e)}
    
    def analyze_folder(self, folder_path: Path, state: str = "unknown") -> List[Dict[str, Any]]:
        """
        Analyze all data files in a folder.
        
        Args:
            folder_path: Path to the folder containing data files
            state: State identifier for the files
            
        Returns:
            List of analysis results for each file
        """
        results = []
        
        # Analyze Parquet files
        for parquet_file in folder_path.glob("*.parquet"):
            result = self.analyze_parquet_file(parquet_file, state)
            results.append(result)
        
        # Analyze CSV files
        for csv_file in folder_path.glob("*.csv"):
            result = self.analyze_csv_file(csv_file, state)
            results.append(result)
        
        return results
    
    def suggest_field_mappings(self, state: str, min_confidence: float = 0.7) -> List[Dict[str, Any]]:
        """
        Suggest field mappings for a state based on field name patterns.
        
        Args:
            state: State to generate suggestions for
            min_confidence: Minimum confidence threshold for suggestions
            
        Returns:
            List of suggested mappings
        """
        suggestions = []
        state_fields = self.field_frequencies.get(state, Counter())
        
        # Get all unified fields
        unified_fields = self.field_library.unified_fields
        
        for state_field, frequency in state_fields.items():
            best_match = None
            best_confidence = 0.0
            
            for unified_field_name, unified_field_def in unified_fields.items():
                confidence = self._calculate_field_similarity(state_field, unified_field_name, unified_field_def)
                
                if confidence > best_confidence and confidence >= min_confidence:
                    best_confidence = confidence
                    best_match = unified_field_name
            
            if best_match:
                suggestions.append({
                    "state": state,
                    "state_field": state_field,
                    "unified_field": best_match,
                    "confidence": best_confidence,
                    "frequency": frequency,
                    "reasoning": self._explain_mapping_reasoning(state_field, best_match, unified_fields[best_match])
                })
        
        # Sort by confidence and frequency
        suggestions.sort(key=lambda x: (x["confidence"], x["frequency"]), reverse=True)
        
        return suggestions
    
    def _calculate_field_similarity(self, state_field: str, unified_field: str, field_def: FieldDefinition) -> float:
        """
        Calculate similarity between a state field and a unified field.
        
        Args:
            state_field: State-specific field name
            unified_field: Unified field name
            field_def: Unified field definition
            
        Returns:
            Similarity score between 0.0 and 1.0
        """
        # Normalize field names for comparison
        state_normalized = self._normalize_field_name(state_field)
        unified_normalized = self._normalize_field_name(unified_field)
        
        # Check exact match
        if state_normalized == unified_normalized:
            return 1.0
        
        # Check if state field matches any examples in unified field
        for example in field_def.examples:
            if self._normalize_field_name(example) == state_normalized:
                return 0.95
        
        # Check partial matches
        if unified_normalized in state_normalized or state_normalized in unified_normalized:
            return 0.8
        
        # Check word overlap
        state_words = set(state_normalized.split('_'))
        unified_words = set(unified_normalized.split('_'))
        
        if state_words and unified_words:
            overlap = len(state_words.intersection(unified_words))
            total = len(state_words.union(unified_words))
            if total > 0:
                return overlap / total * 0.6
        
        # Check semantic patterns
        semantic_patterns = {
            "amount": ["amount", "amt", "sum", "total", "value"],
            "date": ["date", "dt", "time", "when"],
            "name": ["name", "nm", "person", "individual"],
            "address": ["addr", "address", "location", "street"],
            "city": ["city", "town", "municipality"],
            "state": ["state", "province", "region"],
            "zip": ["zip", "postal", "code"],
            "id": ["id", "identifier", "number", "num"],
            "type": ["type", "category", "kind"],
            "description": ["desc", "description", "purpose", "reason"]
        }
        
        for semantic_key, semantic_words in semantic_patterns.items():
            if semantic_key in unified_normalized:
                for word in semantic_words:
                    if word in state_normalized:
                        return 0.7
        
        return 0.0
    
    def _normalize_field_name(self, field_name: str) -> str:
        """
        Normalize a field name for comparison.
        
        Args:
            field_name: Original field name
            
        Returns:
            Normalized field name
        """
        # Convert to lowercase
        normalized = field_name.lower()
        
        # Remove common prefixes/suffixes
        normalized = re.sub(r'^(contributor|payee|filer|recipient|treas|chair)', '', normalized)
        normalized = re.sub(r'(cd|id|dt|addr|name|descr|amount|type)$', '', normalized)
        
        # Replace common separators with underscores
        normalized = re.sub(r'[^a-z0-9]', '_', normalized)
        
        # Remove multiple underscores
        normalized = re.sub(r'_+', '_', normalized)
        
        # Remove leading/trailing underscores
        normalized = normalized.strip('_')
        
        return normalized
    
    def _explain_mapping_reasoning(self, state_field: str, unified_field: str, field_def: FieldDefinition) -> str:
        """
        Generate an explanation for why a mapping was suggested.
        
        Args:
            state_field: State-specific field name
            unified_field: Unified field name
            field_def: Unified field definition
            
        Returns:
            Explanation string
        """
        state_normalized = self._normalize_field_name(state_field)
        unified_normalized = self._normalize_field_name(unified_field)
        
        if state_normalized == unified_normalized:
            return f"Exact match after normalization"
        
        for example in field_def.examples:
            if self._normalize_field_name(example) == state_normalized:
                return f"Matches example field: {example}"
        
        if unified_normalized in state_normalized:
            return f"State field contains unified field name"
        elif state_normalized in unified_normalized:
            return f"Unified field contains state field name"
        
        # Check word overlap
        state_words = set(state_normalized.split('_'))
        unified_words = set(unified_normalized.split('_'))
        
        if state_words and unified_words:
            overlap = state_words.intersection(unified_words)
            if overlap:
                return f"Word overlap: {', '.join(overlap)}"
        
        return "Semantic pattern match"
    
    def generate_field_summary(self) -> Dict[str, Any]:
        """
        Generate a summary of all analyzed fields.
        
        Returns:
            Summary dictionary
        """
        summary = {
            "total_states": len(self.field_frequencies),
            "total_unique_fields": len(set().union(*[set(fields.keys()) for fields in self.field_frequencies.values()])),
            "states": {},
            "field_categories": defaultdict(list),
            "unmapped_fields": []
        }
        
        # Analyze each state
        for state, field_counts in self.field_frequencies.items():
            state_summary = {
                "total_files": sum(field_counts.values()),
                "unique_fields": len(field_counts),
                "most_common_fields": field_counts.most_common(10),
                "mapped_fields": 0,
                "unmapped_fields": []
            }
            
            # Check which fields are mapped
            for field_name in field_counts.keys():
                if self.field_library.map_state_field_to_unified(state, field_name):
                    state_summary["mapped_fields"] += 1
                else:
                    state_summary["unmapped_fields"].append(field_name)
                    summary["unmapped_fields"].append({
                        "state": state,
                        "field": field_name,
                        "frequency": field_counts[field_name]
                    })
            
            summary["states"][state] = state_summary
        
        # Categorize unmapped fields
        for unmapped in summary["unmapped_fields"]:
            field_name = unmapped["field"]
            category = self._categorize_field(field_name)
            summary["field_categories"][category].append(unmapped)
        
        return summary
    
    def _categorize_field(self, field_name: str) -> str:
        """
        Categorize a field based on its name patterns.
        
        Args:
            field_name: Field name to categorize
            
        Returns:
            Category string
        """
        field_lower = field_name.lower()
        
        # Transaction fields
        if any(word in field_lower for word in ["amount", "amt", "sum", "total", "value"]):
            return "amount"
        elif any(word in field_lower for word in ["date", "dt", "time", "when"]):
            return "date"
        elif any(word in field_lower for word in ["id", "identifier", "number", "num"]):
            return "identifier"
        elif any(word in field_lower for word in ["desc", "description", "purpose", "reason"]):
            return "description"
        elif any(word in field_lower for word in ["type", "category", "kind"]):
            return "type"
        
        # Person fields
        elif any(word in field_lower for word in ["name", "nm", "person", "individual"]):
            return "person"
        elif any(word in field_lower for word in ["employer", "occupation", "job", "work"]):
            return "employment"
        
        # Address fields
        elif any(word in field_lower for word in ["addr", "address", "location", "street"]):
            return "address"
        elif any(word in field_lower for word in ["city", "town", "municipality"]):
            return "city"
        elif any(word in field_lower for word in ["state", "province", "region"]):
            return "state"
        elif any(word in field_lower for word in ["zip", "postal", "code"]):
            return "postal_code"
        
        # Committee fields
        elif any(word in field_lower for word in ["committee", "comm", "filer", "treas"]):
            return "committee"
        
        # Administrative fields
        elif any(word in field_lower for word in ["filed", "filing", "report", "form"]):
            return "filing"
        elif any(word in field_lower for word in ["amend", "correct", "update"]):
            return "amendment"
        
        else:
            return "other"
    
    def export_analysis(self, output_path: Path):
        """
        Export the analysis results to a JSON file.
        
        Args:
            output_path: Path to save the analysis results
        """
        analysis_data = {
            "field_frequencies": {
                state: dict(field_counts) 
                for state, field_counts in self.field_frequencies.items()
            },
            "suggested_mappings": self.suggested_mappings,
            "summary": self.generate_field_summary()
        }
        
        import json
        with open(output_path, 'w') as f:
            json.dump(analysis_data, f, indent=2)


def analyze_campaign_finance_fields():
    """
    Main function to analyze campaign finance fields across all states.
    """
    from .unified_field_library import field_library
    
    analyzer = FieldAnalyzer(field_library)
    
    # Analyze Texas data
    texas_folder = Path("tmp/texas")
    if texas_folder.exists():
        ic("Analyzing Texas data...")
        texas_results = analyzer.analyze_folder(texas_folder, "texas")
        ic(f"Analyzed {len(texas_results)} Texas files")
    
    # Analyze Oklahoma data
    oklahoma_folder = Path("tmp/oklahoma")
    if oklahoma_folder.exists():
        ic("Analyzing Oklahoma data...")
        oklahoma_results = analyzer.analyze_folder(oklahoma_folder, "oklahoma")
        ic(f"Analyzed {len(oklahoma_results)} Oklahoma files")
    
    # Generate suggestions
    ic("Generating field mapping suggestions...")
    for state in ["texas", "oklahoma"]:
        suggestions = analyzer.suggest_field_mappings(state, min_confidence=0.5)
        ic(f"Generated {len(suggestions)} suggestions for {state}")
        
        # Show top suggestions
        for suggestion in suggestions[:5]:
            ic(f"  {suggestion['state_field']} -> {suggestion['unified_field']} (confidence: {suggestion['confidence']:.2f})")
    
    # Generate summary
    summary = analyzer.generate_field_summary()
    ic("Field Analysis Summary:")
    ic(f"  Total states: {summary['total_states']}")
    ic(f"  Total unique fields: {summary['total_unique_fields']}")
    ic(f"  Unmapped fields: {len(summary['unmapped_fields'])}")
    
    # Export results
    output_path = Path("field_analysis_results.json")
    analyzer.export_analysis(output_path)
    ic(f"Analysis exported to {output_path}")
    
    return analyzer


if __name__ == "__main__":
    analyze_campaign_finance_fields() 