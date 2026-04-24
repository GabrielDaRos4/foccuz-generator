import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.gocar import (
    RepMatcher,
)


class TestRepMatcher:

    @pytest.fixture
    def users_mapping(self):
        return {
            "AIMME AGUILAR": "1",
            "KARLA RUIZ": "5",
            "JONATHAN CEDENO": "4",
            "ANGEL GUTIERREZ": "17",
        }

    @pytest.fixture
    def matcher(self, users_mapping):
        return RepMatcher(users_mapping)


class TestFindRepId(TestRepMatcher):

    def test_should_return_none_for_none_input(self, matcher):
        result = matcher.find_rep_id(None)

        assert result is None

    def test_should_return_none_for_nan_input(self, matcher):
        result = matcher.find_rep_id(float('nan'))

        assert result is None


class TestExactMatch(TestRepMatcher):

    def test_should_match_exact_name(self, matcher):
        result = matcher.find_rep_id("AIMME AGUILAR")

        assert result == "1"

    def test_should_match_case_insensitive(self, matcher):
        result = matcher.find_rep_id("aimme aguilar")

        assert result == "1"

    def test_should_match_with_extra_spaces(self, matcher):
        result = matcher.find_rep_id("  AIMME   AGUILAR  ")

        assert result == "1"


class TestPartialMatch(TestRepMatcher):

    def test_should_match_partial_name_when_excel_contains_user(self, matcher):
        result = matcher.find_rep_id("AIMME CHRISTIAN AGUILAR VAZQUEZ")

        assert result == "1"


class TestKeywordMatch(TestRepMatcher):

    def test_should_match_when_all_user_words_in_excel(self, matcher):
        result = matcher.find_rep_id("KARLA RUIZ PALACIOS")

        assert result == "5"


class TestFirstNameSurnameMatch(TestRepMatcher):

    def test_should_match_first_name_and_surname(self, matcher):
        result = matcher.find_rep_id("ANGEL GUTIERREZ ORTIZ")

        assert result == "17"


class TestFuzzyMatch(TestRepMatcher):

    def test_should_match_similar_names_above_threshold(self, matcher):
        result = matcher.find_rep_id("KARLA RUIZZ")

        assert result == "5"

    def test_should_not_match_very_different_names(self, matcher):
        result = matcher.find_rep_id("COMPLETELY DIFFERENT NAME")

        assert result is None


class TestNormalizeName(TestRepMatcher):

    def test_should_remove_accents(self):
        result = RepMatcher._normalize_name("JOSÉ GARCÍA")

        assert result == "JOSE GARCIA"

    def test_should_replace_n_tilde(self):
        result = RepMatcher._normalize_name("PEÑA")

        assert result == "PENA"

    def test_should_remove_dots_and_commas(self):
        result = RepMatcher._normalize_name("Dr. JUAN, JR.")

        assert result == "DR JUAN JR"

    def test_should_normalize_multiple_spaces(self):
        result = RepMatcher._normalize_name("JUAN   CARLOS    PEREZ")

        assert result == "JUAN CARLOS PEREZ"


class TestUnificationMapping(TestRepMatcher):

    def test_should_apply_unification_mapping(self):
        users_mapping = {"JUAN PEREZ": "10", "JUAN PEREZ DUPLICADO": "20"}
        unification_mapping = {"20": "10"}
        matcher = RepMatcher(users_mapping, unification_mapping)

        result = matcher.find_rep_id("JUAN PEREZ DUPLICADO")

        assert result == "10"


class TestFromDataframe(TestRepMatcher):

    def test_should_create_matcher_from_dataframe(self):
        df = pd.DataFrame({
            "Rep ID": [1, 2, 3],
            "Nombre": ["JUAN PEREZ", "MARIA LOPEZ", "CARLOS GARCIA"]
        })

        matcher = RepMatcher.from_dataframe(df)

        assert matcher.find_rep_id("JUAN PEREZ") == "1"
        assert matcher.find_rep_id("MARIA LOPEZ") == "2"
        assert matcher.find_rep_id("CARLOS GARCIA") == "3"
