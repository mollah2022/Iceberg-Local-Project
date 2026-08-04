"""
Loads static reference/mapping JSON files from disk.
Implements the MappingRepository protocol (see interfaces.py).
Single Responsibility: reference-data loading and caching only.
"""
import json
from pathlib import Path
from typing import Dict, Optional

from scripts.exceptions import MappingLoadError
from scripts.logging_config import get_logger

logger = get_logger(__name__)


class JsonMappingLoader:
    """Loads a JSON mapping file and exposes it as a plain dict. Caches after first load."""

    def __init__(self, mapping_file_path: Path):
        self._mapping_file_path = mapping_file_path
        self._cache: Optional[Dict[str, str]] = None

    def load(self) -> Dict[str, str]:
        if self._cache is not None:
            return self._cache

        if not self._mapping_file_path.exists():
            raise MappingLoadError(f"Mapping file not found: {self._mapping_file_path}")

        try:
            with open(self._mapping_file_path, "r", encoding="utf-8") as f:
                self._cache = json.load(f)
        except json.JSONDecodeError as exc:
            raise MappingLoadError(
                f"Invalid JSON in mapping file {self._mapping_file_path}: {exc}"
            ) from exc

        logger.info("Loaded mapping file: %s (%d entries)", self._mapping_file_path, len(self._cache))
        return self._cache


def get_mappings_dir() -> Path:
    project_root = Path(__file__).resolve().parent.parent
    return project_root / "mappings"