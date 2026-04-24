import calendar
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd
import requests

from src.context.commissions.domain.repositories import DataRepository
from src.context.commissions.domain.value_objects import DataSourceConfig

logger = logging.getLogger(__name__)


class BuKDataRepository(DataRepository):

    DEFAULT_PAGE_SIZE = 500
    DEFAULT_TIMEOUT = 90
    MAX_WORKERS = 10
    PAYROLL_DETAIL_ENDPOINT = "/employees/{employee_id}/payroll_detail"

    def __init__(self, api_token: str | None = None):
        self.api_token = api_token
        self._session = None

    def _get_session(self) -> requests.Session:
        if self._session is None:
            self._session = requests.Session()
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=self.MAX_WORKERS,
                pool_maxsize=self.MAX_WORKERS
            )
            self._session.mount('http://', adapter)
            self._session.mount('https://', adapter)
        return self._session

    def get_data_for_plan(self, source: DataSourceConfig) -> pd.DataFrame:
        config = source.config
        api_token = config.get('api_token', config.get('auth_token', self.api_token))

        endpoint = config.get('endpoint') or config.get('api_endpoint')
        params = config.get('params', {}) or config.get('filters', {})

        if not endpoint:
            raise ValueError("BuK configuration must include 'endpoint'")

        if not api_token:
            raise ValueError("BuK API token not configured")

        try:
            logger.info(f"Fetching data from BuK API: {endpoint}")

            headers = self._build_headers(config, api_token)

            if config.get('paginated', True):
                all_data = self._fetch_paginated_concurrent(endpoint, headers, params, config)
            else:
                all_data = self._fetch_single(endpoint, headers, params)

            df = pd.DataFrame(all_data) if all_data else pd.DataFrame()

            logger.info(f"Loaded {len(df)} rows from BuK API")
            return df

        except Exception as e:
            logger.error(f"Error calling BuK API {endpoint}: {str(e)}")
            raise

    @staticmethod
    def _build_headers(config: dict, api_token: str) -> dict:
        auth_type = config.get('auth_type', 'auth_token')

        if auth_type == 'bearer':
            return {
                'Authorization': f'Bearer {api_token}',
                'Content-Type': 'application/json'
            }

        return {
            'auth_token': api_token,
            'Content-Type': 'application/json'
        }

    def _fetch_single(self, url: str, headers: dict, params: dict) -> list[dict]:
        session = self._get_session()
        response = session.get(
            url,
            headers=headers,
            params=params,
            timeout=self.DEFAULT_TIMEOUT
        )
        response.raise_for_status()
        data = response.json()

        if isinstance(data, list):
            return data
        if isinstance(data, dict) and 'data' in data:
            return data['data']
        return [data]

    def _fetch_page(
        self,
        url: str,
        headers: dict,
        base_params: dict,
        page: int,
        page_size: int
    ) -> tuple[int, list[dict]]:
        session = self._get_session()
        params = {**base_params, 'page': page, 'per_page': page_size}

        response = session.get(
            url,
            headers=headers,
            params=params,
            timeout=self.DEFAULT_TIMEOUT
        )
        response.raise_for_status()
        response_json = response.json()

        page_data = response_json.get('data', [])
        return page, page_data if isinstance(page_data, list) else []

    def _fetch_paginated_concurrent(
        self,
        url: str,
        headers: dict,
        base_params: dict,
        config: dict
    ) -> list[dict]:
        session = self._get_session()
        page_size = config.get('page_size', self.DEFAULT_PAGE_SIZE)

        first_params = {**base_params, 'page': 1, 'per_page': page_size}
        response = session.get(
            url,
            headers=headers,
            params=first_params,
            timeout=self.DEFAULT_TIMEOUT
        )
        response.raise_for_status()
        first_response = response.json()

        pagination = first_response.get('pagination', {})
        total_pages = pagination.get('total_pages', 1)
        total_count = pagination.get('count', 0)

        logger.info(f"BuK API: {total_count} records in {total_pages} pages")

        first_data = first_response.get('data', [])
        if not isinstance(first_data, list):
            return []

        if total_pages == 1:
            return first_data

        all_data = {1: first_data}

        remaining_pages = list(range(2, total_pages + 1))
        max_workers = min(self.MAX_WORKERS, len(remaining_pages))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    self._fetch_page, url, headers, base_params, page, page_size
                ): page
                for page in remaining_pages
            }

            for future in as_completed(futures):
                page_num, page_data = future.result()
                all_data[page_num] = page_data
                logger.debug(f"Page {page_num}: {len(page_data)} records")

        result = []
        for page_num in sorted(all_data.keys()):
            result.extend(all_data[page_num])

        logger.info(f"Fetched {len(result)} total records from BuK API (concurrent)")
        return result

    def enrich_with_payroll_details(
        self,
        employees_df: pd.DataFrame,
        base_url: str,
        api_token: str,
        period: str = None
    ) -> pd.DataFrame:
        if employees_df.empty:
            return employees_df

        result = employees_df.copy()
        result["days_worked"] = None
        result["garantizado"] = None

        rut_col = self._find_rut_column(result)
        if not rut_col:
            logger.warning("No RUT column found, cannot fetch payroll details")
            return result

        ruts = result[rut_col].dropna().unique().tolist()
        logger.info(f"Fetching payroll details for {len(ruts)} employees")

        payroll_data = self._fetch_payroll_details_concurrent(
            ruts, base_url, api_token, period
        )

        for rut, details in payroll_data.items():
            normalized_rut = self._normalize_rut_for_lookup(rut)
            mask = result[rut_col].apply(
                lambda x, nr=normalized_rut: self._normalize_rut_for_lookup(str(x)) == nr
            )
            result.loc[mask, "days_worked"] = details.get("days_worked")
            result.loc[mask, "garantizado"] = details.get("garantizado", 0)

        return result

    def _find_rut_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            if "rut" in col.lower():
                return col
        return None

    def _normalize_rut_for_lookup(self, rut: str) -> str:
        return rut.replace(".", "").replace(" ", "").strip()

    def _build_payroll_period_params(self, period: str | None) -> dict:
        if not period:
            return {}

        try:
            period_date = datetime.strptime(period, "%Y-%m-%d")
        except ValueError:
            logger.warning(f"Invalid period format: {period}, expected YYYY-MM-DD")
            return {}

        year = period_date.year
        month = period_date.month
        last_day = calendar.monthrange(year, month)[1]

        start_date = f"01-{month:02d}-{year}"
        end_date = f"{last_day:02d}-{month:02d}-{year}"

        logger.debug(f"Payroll period params: start={start_date}, end={end_date}")
        return {"start": start_date, "end": end_date}

    def _fetch_payroll_details_concurrent(
        self,
        ruts: list[str],
        base_url: str,
        api_token: str,
        period: str = None
    ) -> dict[str, dict]:
        payroll_data = {}

        headers = {"auth_token": api_token, "Content-Type": "application/json"}
        params = self._build_payroll_period_params(period)

        max_workers = min(self.MAX_WORKERS, len(ruts))
        if max_workers == 0:
            return payroll_data

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    self._fetch_single_payroll_detail,
                    rut, base_url, headers, params
                ): rut
                for rut in ruts
            }

            for future in as_completed(futures):
                rut = futures[future]
                try:
                    details = future.result()
                    if details is not None:
                        payroll_data[rut] = details
                except Exception as e:
                    logger.warning(f"Failed to fetch payroll for {rut}: {e}")

        logger.info(f"Fetched payroll details for {len(payroll_data)} employees")
        return payroll_data

    def _fetch_single_payroll_detail(
        self,
        rut: str,
        base_url: str,
        headers: dict,
        params: dict
    ) -> dict | None:
        normalized_rut = self._normalize_rut_for_lookup(rut)
        url = f"{base_url}{self.PAYROLL_DETAIL_ENDPOINT.format(employee_id=normalized_rut)}"

        try:
            session = self._get_session()
            response = session.get(
                url, headers=headers, params=params, timeout=self.DEFAULT_TIMEOUT
            )
            response.raise_for_status()
            data = response.json()

            return self._extract_payroll_fields(data)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.debug(f"No payroll detail found for {rut}")
            else:
                logger.warning(f"HTTP error fetching payroll for {rut}: {e}")
            return None
        except Exception as e:
            logger.warning(f"Error fetching payroll for {rut}: {e}")
            return None

    def _extract_payroll_fields(self, payroll_data: dict | list) -> dict | None:
        if isinstance(payroll_data, list):
            if payroll_data:
                return self._extract_payroll_fields(payroll_data[0])
            return None

        if isinstance(payroll_data, dict):
            if "data" in payroll_data:
                return self._extract_payroll_fields(payroll_data["data"])

            result = {}

            days = self._extract_days_worked(payroll_data)
            if days is not None:
                result["days_worked"] = days

            garantizado = self._extract_garantizado(payroll_data)
            if garantizado is not None:
                result["garantizado"] = garantizado

            return result if result else None

        return None

    def _extract_days_worked(self, payroll_data: dict | list) -> int | None:
        if isinstance(payroll_data, list):
            if payroll_data:
                return self._extract_days_worked(payroll_data[0])
            return None

        if isinstance(payroll_data, dict):
            if "worked_days" in payroll_data:
                return int(payroll_data["worked_days"])
            if "days_worked" in payroll_data:
                return int(payroll_data["days_worked"])
            if "dias_trabajados" in payroll_data:
                return int(payroll_data["dias_trabajados"])

        return None

    def _extract_garantizado(self, payroll_data: dict | list) -> int | None:
        if isinstance(payroll_data, list):
            if payroll_data:
                return self._extract_garantizado(payroll_data[0])
            return None

        if isinstance(payroll_data, dict):
            for key in ["garantizado", "guaranteed", "minimo_garantizado", "guaranteed_minimum"]:
                if key in payroll_data:
                    try:
                        value = payroll_data[key]
                        if value is None or str(value).upper() in ["NA", "N/A", ""]:
                            return 0
                        return int(float(value))
                    except (ValueError, TypeError):
                        return 0

        return None

