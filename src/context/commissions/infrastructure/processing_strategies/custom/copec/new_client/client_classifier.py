import logging

import pandas as pd

from .client_classification import ClientClassification

logger = logging.getLogger(__name__)


class ClientClassifier:

    CLIENT_RUT_COL = "client_rut_complete"

    def classify(
        self,
        df: pd.DataFrame,
        clients_m1: set[str],
        clients_m2: set[str],
        clients_historical: set[str]
    ) -> pd.DataFrame:
        df = df.copy()

        df["_classification"] = df[self.CLIENT_RUT_COL].apply(
            lambda rut: self._classify_single(rut, clients_m1, clients_m2, clients_historical)
        )
        df["is_new_client"] = df["_classification"].apply(lambda c: c.is_new)
        df["gets_bonus"] = df["_classification"].apply(lambda c: c.gets_bonus)

        new_clients = df[df["is_new_client"]].copy()
        self._log_classification_results(new_clients)

        return new_clients

    @staticmethod
    def _classify_single(
        rut: str,
        clients_m1: set,
        clients_m2: set,
        clients_historical: set
    ) -> ClientClassification:
        if rut in clients_historical:
            return ClientClassification(is_new=False, gets_bonus=False)

        if rut not in clients_m1 and rut not in clients_m2:
            return ClientClassification(is_new=True, gets_bonus=True)

        return ClientClassification(is_new=True, gets_bonus=False)

    def _log_classification_results(self, df: pd.DataFrame) -> None:
        total = df[self.CLIENT_RUT_COL].nunique()
        with_bonus = df[df["gets_bonus"]][self.CLIENT_RUT_COL].nunique()
        logger.info(f"New clients: {total}, with bonus: {with_bonus}, without: {total - with_bonus}")
