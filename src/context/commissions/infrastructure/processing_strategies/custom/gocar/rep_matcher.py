import logging
from difflib import SequenceMatcher

import pandas as pd

logger = logging.getLogger(__name__)


class RepMatcher:
    FUZZY_MATCH_THRESHOLD = 0.75

    def __init__(self, users_mapping: dict[str, str], unification_mapping: dict[str, str] = None):
        self._users_mapping = users_mapping
        self._unification_mapping = unification_mapping or {}

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame, unification_map: dict = None) -> 'RepMatcher':
        users_map = {}
        for _, row in df.iterrows():
            normalized_name = cls._normalize_name(str(row["Nombre"]))
            users_map[normalized_name] = str(row["Rep ID"])
        return cls(users_map, unification_map)

    def find_rep_id(self, seller_name: str) -> str | None:
        if pd.isna(seller_name):
            return None

        normalized = self._normalize_name(seller_name)
        rep_id = self._try_exact_match(normalized)

        if not rep_id:
            rep_id = self._try_partial_match(normalized)

        if not rep_id:
            rep_id = self._try_keyword_match(normalized)

        if not rep_id:
            rep_id = self._try_first_name_surname_match(normalized)

        if not rep_id:
            rep_id = self._try_fuzzy_match(normalized)

        if rep_id and str(rep_id) in self._unification_mapping:
            return self._unification_mapping[str(rep_id)]

        return rep_id

    def _try_exact_match(self, normalized: str) -> str | None:
        return self._users_mapping.get(normalized)

    def _try_partial_match(self, normalized: str) -> str | None:
        for user_name, rep_id in self._users_mapping.items():
            if user_name in normalized or normalized in user_name:
                return rep_id
        return None

    def _try_keyword_match(self, normalized: str) -> str | None:
        words = normalized.split()
        for user_name, rep_id in self._users_mapping.items():
            user_words = user_name.split()
            if all(word in words for word in user_words):
                return rep_id
        return None

    def _try_first_name_surname_match(self, normalized: str) -> str | None:
        words = normalized.split()
        if len(words) < 2:
            return None

        first_name = words[0]
        first_surname = words[1]

        for user_name, rep_id in self._users_mapping.items():
            user_words = user_name.split()
            if len(user_words) < 2:
                continue

            if user_words[0] == first_name and user_words[1] == first_surname:
                return rep_id

            if user_words[0] == first_name and first_surname in user_words:
                return rep_id

        return None

    def _try_fuzzy_match(self, normalized: str) -> str | None:
        best_match = None
        best_score = self.FUZZY_MATCH_THRESHOLD

        for user_name, rep_id in self._users_mapping.items():
            score = SequenceMatcher(None, normalized, user_name).ratio()
            if score > best_score:
                best_score = score
                best_match = rep_id

        return best_match

    @staticmethod
    def _normalize_name(name: str) -> str:
        if pd.isna(name):
            return ""

        name = str(name).upper().strip()

        replacements = {
            "Á": "A", "É": "E", "Í": "I", "Ó": "O", "Ú": "U",
            "Ñ": "N", "Ü": "U", ".": "", ",": "", "-": " ",
        }

        for old, new in replacements.items():
            name = name.replace(old, new)

        return " ".join(name.split())
