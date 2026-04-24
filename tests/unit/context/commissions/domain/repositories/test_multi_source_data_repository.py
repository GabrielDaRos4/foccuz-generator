import pandas as pd

from src.context.commissions.domain.repositories import MultiSourceDataRepository
from tests.mothers.commissions.domain.aggregates_mother import PlanMother


class StubMultiSourceRepo(MultiSourceDataRepository):
    def __init__(self, single_data=None, multi_data=None, merge_data=None):
        self._single_data = single_data if single_data is not None else pd.DataFrame()
        self._multi_data = multi_data if multi_data is not None else {}
        self._merge_data = merge_data if merge_data is not None else pd.DataFrame()

    def fetch_single_source(self, source):
        return self._single_data

    def fetch_multiple_sources(self, collection):
        return self._multi_data

    def merge_sources(self, dataframes, collection, plan_params=None):
        return self._merge_data


class TestMultiSourceDataRepository:

    def test_should_fetch_single_source_for_single_plan(self):
        data = pd.DataFrame({"col": [1, 2]})
        repo = StubMultiSourceRepo(single_data=data)
        plan = PlanMother.active()

        result = repo.get_data_for_plan(plan)

        assert len(result) == 2

    def test_should_merge_for_multi_source_plan(self):
        merged = pd.DataFrame({"col": [1, 2, 3]})
        repo = StubMultiSourceRepo(
            multi_data={"a": pd.DataFrame(), "b": pd.DataFrame()},
            merge_data=merged,
        )
        plan = PlanMother.with_multi_source()

        result = repo.get_data_for_plan(plan)

        assert len(result) == 3
