import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.grupo_vanguardia.monedero import (
    MonederoCommissionStrategy,
)


class TestCalculateCommission:

    def test_should_return_empty_for_empty_data(self):
        strategy = MonederoCommissionStrategy()
        result = strategy.calculate_commission(pd.DataFrame())
        assert result.empty

    def test_should_filter_brands_correctly(self):
        strategy = MonederoCommissionStrategy()
        data = pd.DataFrame({
            "Brand": ["HONDA", "TOYOTA", "ACURA", "BMW"]
        })
        result = strategy._filter_brands(data)
        assert len(result) == 2
        assert "TOYOTA" not in result["Brand"].values
        assert "BMW" not in result["Brand"].values


class TestAddCommissionType:

    def test_should_assign_tabla_1_for_1_car(self):
        strategy = MonederoCommissionStrategy()
        data = pd.DataFrame({"cars_delivered": [1]})
        result = strategy._add_commission_type(data)
        assert result["Tipo de Comision"].iloc[0] == "Tabla 1"

    def test_should_assign_tabla_1_for_2_cars(self):
        strategy = MonederoCommissionStrategy()
        data = pd.DataFrame({"cars_delivered": [2]})
        result = strategy._add_commission_type(data)
        assert result["Tipo de Comision"].iloc[0] == "Tabla 1"

    def test_should_assign_tabla_2_for_3_cars(self):
        strategy = MonederoCommissionStrategy()
        data = pd.DataFrame({"cars_delivered": [3]})
        result = strategy._add_commission_type(data)
        assert result["Tipo de Comision"].iloc[0] == "Tabla 2"

    def test_should_assign_tabla_2_for_4_cars(self):
        strategy = MonederoCommissionStrategy()
        data = pd.DataFrame({"cars_delivered": [4]})
        result = strategy._add_commission_type(data)
        assert result["Tipo de Comision"].iloc[0] == "Tabla 2"

    def test_should_assign_tabla_3_for_5_cars(self):
        strategy = MonederoCommissionStrategy()
        data = pd.DataFrame({"cars_delivered": [5]})
        result = strategy._add_commission_type(data)
        assert result["Tipo de Comision"].iloc[0] == "Tabla 3"

    def test_should_assign_tabla_3_for_more_than_5_cars(self):
        strategy = MonederoCommissionStrategy()
        data = pd.DataFrame({"cars_delivered": [10]})
        result = strategy._add_commission_type(data)
        assert result["Tipo de Comision"].iloc[0] == "Tabla 3"


class TestAddCarsDeliveredCount:

    def test_should_count_cars_per_rep_and_month(self):
        strategy = MonederoCommissionStrategy()
        data = pd.DataFrame({
            "Id": ["T001", "T002", "T003", "T004"],
            "Rep ID": ["REP001", "REP001", "REP002", "REP001"],
            "Delivery_Month": ["2025-05", "2025-05", "2025-05", "2025-06"]
        })
        result = strategy._add_cars_delivered_count(data)

        rep001_may = result[(result["Rep ID"] == "REP001") & (result["Delivery_Month"] == "2025-05")]
        assert rep001_may["cars_delivered"].iloc[0] == 2

        rep002_may = result[(result["Rep ID"] == "REP002") & (result["Delivery_Month"] == "2025-05")]
        assert rep002_may["cars_delivered"].iloc[0] == 1

        rep001_june = result[(result["Rep ID"] == "REP001") & (result["Delivery_Month"] == "2025-06")]
        assert rep001_june["cars_delivered"].iloc[0] == 1


class TestBuildCarModel:

    def test_should_build_model_with_version_and_year(self):
        strategy = MonederoCommissionStrategy()
        data = pd.DataFrame({
            "Model": ["CR-V"],
            "Version": ["EX"],
            "Year": [2025]
        })
        result = strategy._add_car_model(data)
        assert result["Car Model"].iloc[0] == "CR-V EX 2025"

    def test_should_build_model_without_version(self):
        strategy = MonederoCommissionStrategy()
        data = pd.DataFrame({
            "Model": ["CR-V"],
            "Year": [2025]
        })
        result = strategy._add_car_model(data)
        assert result["Car Model"].iloc[0] == "CR-V 2025"

    def test_should_build_model_without_year(self):
        strategy = MonederoCommissionStrategy()
        data = pd.DataFrame({
            "Model": ["CR-V"],
            "Version": ["EX"]
        })
        result = strategy._add_car_model(data)
        assert result["Car Model"].iloc[0] == "CR-V EX"

    def test_should_handle_missing_model_column(self):
        strategy = MonederoCommissionStrategy()
        data = pd.DataFrame({"Other": ["value"]})
        result = strategy._add_car_model(data)
        assert result["Car Model"].iloc[0] == ""


class TestFilterByDeliveryDate:

    def test_should_filter_sales_from_2025(self):
        strategy = MonederoCommissionStrategy()
        data = pd.DataFrame({
            "Delivery_Date": ["2024-12-15", "2025-01-15", "2025-05-20"],
            "Year": [2024, 2025, 2025]
        })
        result = strategy._filter_by_delivery_date(data)
        assert len(result) == 2

    def test_should_filter_by_car_year_2024_or_later(self):
        strategy = MonederoCommissionStrategy()
        data = pd.DataFrame({
            "Delivery_Date": ["2025-05-15", "2025-05-20"],
            "Year": [2023, 2025]
        })
        result = strategy._filter_by_delivery_date(data)
        assert len(result) == 1


class TestFilterValidRepIds:

    def test_should_filter_empty_rep_ids(self):
        strategy = MonederoCommissionStrategy()
        data = pd.DataFrame({
            "Rep ID": ["REP001", "", None, " ", "REP002"]
        })
        result = strategy._filter_valid_rep_ids(data)
        assert len(result) == 2
        assert "REP001" in result["Rep ID"].values
        assert "REP002" in result["Rep ID"].values

    def test_should_strip_rep_ids(self):
        strategy = MonederoCommissionStrategy()
        data = pd.DataFrame({
            "Rep ID": ["  REP001  ", "REP002"]
        })
        result = strategy._filter_valid_rep_ids(data)
        assert result["Rep ID"].iloc[0] == "REP001"


class TestFormatOutput:

    def test_should_include_required_columns(self):
        strategy = MonederoCommissionStrategy()
        data = pd.DataFrame({
            "Id": ["T001"],
            "IdOrder": ["O001"],
            "Status": ["Delivered"],
            "idAgency": ["A1"],
            "Invoice": ["INV001"],
            "Delivery_Date": pd.to_datetime(["2025-05-15"]),
            "VIN": ["VIN001"],
            "Brand": ["HONDA"],
            "Year": [2025],
            "Model": ["CR-V"],
            "Version": ["EX"],
            "Customer_Name": ["John"],
            "Rep ID": ["REP001"],
            "Delivery_Month": ["2025-05"],
            "Tipo de Comision": ["Tabla 1"],
            "Comision": [5000]
        })
        result = strategy._format_output(data)
        assert "ID Transaccion" in result.columns
        assert "Fecha" in result.columns
        assert "Rep ID" in result.columns
        assert "Comision" in result.columns

    def test_should_add_column_types_attr(self):
        strategy = MonederoCommissionStrategy()
        data = pd.DataFrame({
            "Id": ["T001"],
            "Delivery_Date": pd.to_datetime(["2025-05-15"]),
            "Brand": ["HONDA"],
            "Rep ID": ["REP001"],
            "Delivery_Month": ["2025-05"],
            "Tipo de Comision": ["Tabla 1"],
            "Comision": [5000]
        })
        result = strategy._format_output(data)
        assert "column_types" in result.attrs
        assert result.attrs["column_types"]["Comision"] == "money"
        assert result.attrs["column_types"]["Fecha"] == "date"

    def test_should_rename_id_to_id_transaccion(self):
        strategy = MonederoCommissionStrategy()
        data = pd.DataFrame({
            "Id": ["T001"],
            "Delivery_Date": pd.to_datetime(["2025-05-15"]),
            "Rep ID": ["REP001"],
            "Delivery_Month": ["2025-05"],
            "Tipo de Comision": ["Tabla 1"],
            "Comision": [5000]
        })
        result = strategy._format_output(data)
        assert "ID Transaccion" in result.columns
        assert "Id" not in result.columns


class TestAddDeliveryMonth:

    def test_should_extract_month_from_delivery_date(self):
        strategy = MonederoCommissionStrategy()
        data = pd.DataFrame({
            "Delivery_Date": pd.to_datetime(["2025-05-15", "2025-06-20", "2025-12-01"])
        })
        result = strategy._add_delivery_month(data)
        assert result["Delivery_Month"].iloc[0] == "2025-05"
        assert result["Delivery_Month"].iloc[1] == "2025-06"
        assert result["Delivery_Month"].iloc[2] == "2025-12"
