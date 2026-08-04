"""
Abstractions that decouple business logic from concrete implementations.
Following the Dependency Inversion Principle: high-level modules (FieldMapper)
depend on this abstraction, not on low-level details (how mappings are loaded).
"""
from typing import Protocol, Dict


class MappingRepository(Protocol):
    """Anything that can provide a key-value mapping dict satisfies this contract."""

    def load(self) -> Dict[str, str]:
        ...