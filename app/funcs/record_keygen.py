from __future__ import annotations
import hashlib
import uuid
from dataclasses import dataclass, field
from typing import Tuple, Any
from datetime import date


@dataclass
class RecordKeyGenerator(object):
    record: str
    hash: hashlib.blake2b.hexdigest = field(init=False)
    uid: uuid.uuid4 = field(init=False)
    __KEY_LENGTH = 16

    def generate_hash(self):
        _hasher = hashlib.blake2b(digest_size=RecordKeyGenerator.__KEY_LENGTH)
        _hasher.update(self.record.encode('utf-8'))
        self.hash = _hasher.hexdigest()
        return self.hash

    def generate_uuid(self):
        self.uid = uuid.uuid4()
        return self.uid

    def __post_init__(self):
        self.generate_hash()
        self.generate_uuid()

    @staticmethod
    def generate_static_key(values: Tuple[Any, ...] | str) -> str:
        def format_value(val: Any) -> str:
            if val is None:
                return 'None'
            elif isinstance(val, (str, int, float, bool)):
                return str(val)
            elif isinstance(val, (list, tuple, set)) and all(isinstance(x, date) for x in val):
                return '_'.join(sorted(d.isoformat() for d in val))
            elif isinstance(val, (list, tuple, set)):
                return '_'.join(format_value(v) for v in val)
            elif isinstance(val, dict):
                return '_'.join(f"{k}:{format_value(v)}" for k, v in sorted(val.items()))
            elif isinstance(val, date):
                return val.isoformat()
            else:
                raise ValueError(f"Unsupported type for hash key generation: {type(val)}")

        if isinstance(values, tuple):
            key_parts = [format_value(value) for value in values]
            key_string = '_'.join(key_parts)
        elif isinstance(values, str):
            key_string = values
        else:
            raise ValueError(f"Unsupported type for hash key generation: {type(values)}")

        return hashlib.sha256(key_string.encode()).hexdigest()[:16]  # Using first 16 characters for brevity