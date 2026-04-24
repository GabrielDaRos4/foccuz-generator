from collections.abc import Sequence

import pandas as pd

ThresholdTuple = tuple[float, float, int]
ThresholdList = Sequence[ThresholdTuple]


class ThresholdCalculator:

    @staticmethod
    def calculate_payment(value: float, thresholds: ThresholdList) -> int:
        if pd.isna(value):
            return 0
        for lower, upper, payment in thresholds:
            if lower <= value <= upper:
                return payment
        return 0

    @staticmethod
    def calculate_payment_series(
        values: pd.Series,
        thresholds: ThresholdList
    ) -> pd.Series:
        return values.apply(
            lambda x: ThresholdCalculator.calculate_payment(x, thresholds)
        )

    @staticmethod
    def calculate_factor(value: float, thresholds: ThresholdList) -> float:
        if pd.isna(value):
            return 1.0
        for lower, upper, factor in thresholds:
            if lower <= value <= upper:
                return factor
        return 1.0

    @staticmethod
    def normalize_percentage(values: pd.Series, threshold: float = 2.0) -> pd.Series:
        if values.median() <= threshold:
            return values
        return values / 100

    @staticmethod
    def to_percentage(values: pd.Series, threshold: float = 2.0) -> pd.Series:
        if values.median() <= threshold:
            return values * 100
        return values
