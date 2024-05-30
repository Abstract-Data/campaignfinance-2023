from __future__ import annotations
import csv
from pathlib import Path
from sqlmodel import SQLModel
from typing import List, Dict, Type, Generator


def write_records_to_csv_validation(
    records: List[Dict | Type[SQLModel]] | Generator[Dict | Type[SQLModel], None, None], 
    folder_path: Path | str,
    record_type: str,
        validation_status: str):
    if not isinstance(folder_path, Path):
        try:
            folder_path = Path(folder_path)
        except TypeError:
            raise ValueError(f"{folder_path} is not a valid path")
        
    if not folder_path.is_dir():
        raise ValueError(f"{folder_path} is not a valid directory")
        
    validation_path_in_folder = folder_path / 'validation' / validation_status
    
    if not validation_path_in_folder.exists():
        validation_path_in_folder.mkdir(parents=True, exist_ok=True)
        
    _filename = validation_path_in_folder / validation_status
    
    chunk_size = 500000  # Adjust this value depending on your needs
    num_chunks = len(records) // chunk_size + 1
    
    for i in range(num_chunks):
        chunk_records = records[i*chunk_size:(i+1)*chunk_size]
        _filename = validation_path_in_folder / f'{record_type}_{validation_status}_{i}.csv'
        with open(_filename, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=chunk_records[0].keys())
            writer.writeheader()
            writer.writerows(chunk_records)