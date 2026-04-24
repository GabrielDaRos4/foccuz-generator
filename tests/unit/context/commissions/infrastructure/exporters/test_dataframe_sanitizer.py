import numpy as np
import pandas as pd

from src.context.commissions.infrastructure.exporters.dataframe_sanitizer import (
    GSHEET_MAX_CELL_LENGTH,
    DataFrameSanitizer,
)


class TestDataFrameSanitizer:

    def test_should_replace_none_with_empty_string(self):
        df = pd.DataFrame({"col": [None, "value"]})

        result = DataFrameSanitizer.sanitize(df)

        assert result["col"].iloc[0] == ""
        assert result["col"].iloc[1] == "value"

    def test_should_replace_nan_with_empty_string(self):
        df = pd.DataFrame({"col": [float("nan"), 42]})

        result = DataFrameSanitizer.sanitize(df)

        assert result["col"].iloc[0] == ""

    def test_should_truncate_long_strings(self):
        long_string = "x" * (GSHEET_MAX_CELL_LENGTH + 100)
        df = pd.DataFrame({"col": [long_string]})

        result = DataFrameSanitizer.sanitize(df)

        assert len(result["col"].iloc[0]) == GSHEET_MAX_CELL_LENGTH

    def test_should_convert_numpy_array_to_string(self):
        df = pd.DataFrame({"col": [np.array([1, 2, 3])]})

        result = DataFrameSanitizer.sanitize(df)

        assert result["col"].iloc[0] == "[1, 2, 3]"

    def test_should_convert_dict_to_string(self):
        df = pd.DataFrame({"col": [{"key": "value"}]})

        result = DataFrameSanitizer.sanitize(df)

        assert result["col"].iloc[0] == "{'key': 'value'}"

    def test_should_convert_list_to_string(self):
        df = pd.DataFrame({"col": [[1, 2, 3]]})

        result = DataFrameSanitizer.sanitize(df)

        assert result["col"].iloc[0] == "[1, 2, 3]"

    def test_should_preserve_normal_values(self):
        df = pd.DataFrame({"col": ["hello", 42, 3.14]})

        result = DataFrameSanitizer.sanitize(df)

        assert result["col"].iloc[0] == "hello"
        assert result["col"].iloc[1] == 42
        assert result["col"].iloc[2] == 3.14

    def test_should_truncate_long_list_to_max_cell_length(self):
        long_list = list(range(GSHEET_MAX_CELL_LENGTH))
        df = pd.DataFrame({"col": [long_list]})

        result = DataFrameSanitizer.sanitize(df)

        assert len(result["col"].iloc[0]) == GSHEET_MAX_CELL_LENGTH

    def test_should_not_mutate_original_dataframe(self):
        df = pd.DataFrame({"col": [None, "value"]})

        DataFrameSanitizer.sanitize(df)

        assert df["col"].iloc[0] is None
