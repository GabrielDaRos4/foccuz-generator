from datetime import datetime

from src.context.commissions.infrastructure.processing_strategies.custom.copec.tct_premium import (
    LicensePlateClassification,
)


class TestLicensePlateClassification:

    def test_new_license_plate_month_0(self):
        classification = LicensePlateClassification(
            is_new=True,
            first_month_offset=0,
            period=datetime(2024, 12, 1)
        )

        assert classification.is_new is True
        assert classification.first_month_offset == 0

    def test_new_license_plate_month_1(self):
        classification = LicensePlateClassification(
            is_new=True,
            first_month_offset=1,
            period=datetime(2024, 12, 1)
        )

        assert classification.is_new is True
        assert classification.first_month_offset == 1

    def test_existing_license_plate(self):
        classification = LicensePlateClassification(
            is_new=False,
            first_month_offset=None,
            period=datetime(2024, 12, 1)
        )

        assert classification.is_new is False

    def test_first_month_detail_returns_month_name(self):
        classification = LicensePlateClassification(
            is_new=True,
            first_month_offset=0,
            period=datetime(2024, 12, 1)
        )

        detail = classification.first_month_detail
        assert "12" in detail
        assert "Diciembre" in detail

    def test_first_month_detail_empty_when_no_period(self):
        classification = LicensePlateClassification(
            is_new=True,
            first_month_offset=0,
            period=None
        )

        assert classification.first_month_detail == ""

    def test_first_month_detail_empty_when_offset_none(self):
        classification = LicensePlateClassification(
            is_new=False,
            first_month_offset=None,
            period=datetime(2024, 12, 1)
        )

        assert classification.first_month_detail == ""
