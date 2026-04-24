import gzip
import io
import logging
import os

import pandas as pd
import requests
from urllib3.exceptions import InsecureRequestWarning

from src.context.commissions.domain.repositories import DataRepository
from src.context.commissions.domain.value_objects import DataSourceConfig

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

logger = logging.getLogger(__name__)


class APIDataRepository(DataRepository):

    DEFAULT_PAGE_SIZE = 1000
    DEFAULT_MAX_PAGES = 100
    DEFAULT_TIMEOUT = 30

    def get_data_for_plan(self, source: DataSourceConfig) -> pd.DataFrame:
        config = source.config
        url = self._build_url(config)
        headers = self._build_headers(config)
        params = config.get("params", {})

        response_format = config.get("response_format", "json")

        if config.get("paginated", False):
            data = self._fetch_paginated(url, headers, params, config)
            return pd.DataFrame(data) if data else pd.DataFrame()

        if response_format == "csv":
            return self._fetch_csv(url, headers, params, config)

        data = self._fetch_single(url, headers, params)
        return pd.DataFrame(data) if data else pd.DataFrame()

    @staticmethod
    def _build_url(config: dict) -> str:
        base_url = config.get("base_url", "")
        path = config.get("path", "")
        return f"{base_url}{path}"

    @staticmethod
    def _build_headers(config: dict) -> dict:
        headers = config.get("headers", {})
        resolved = {}

        for key, value in headers.items():
            if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
                env_var = value[2:-1]
                resolved[key] = os.environ.get(env_var, "")
            else:
                resolved[key] = value

        return resolved

    def _fetch_single(self, url: str, headers: dict, params: dict) -> list[dict]:
        try:
            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=self.DEFAULT_TIMEOUT,
                verify=False
            )
            response.raise_for_status()
            data = response.json()

            if isinstance(data, list):
                return data
            return [data]

        except requests.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise

    def _fetch_csv(
        self,
        url: str,
        headers: dict,
        params: dict,
        config: dict
    ) -> pd.DataFrame:
        try:
            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=config.get("timeout", self.DEFAULT_TIMEOUT),
                verify=False
            )
            response.raise_for_status()

            content = response.content

            if self._is_gzip_content(content):
                content = gzip.decompress(content)
                logger.info("Decompressed gzip response")

            if isinstance(content, bytes):
                encoding = config.get("encoding", "utf-8")
                text_content = content.decode(encoding)
            else:
                text_content = content

            separator = config.get("separator", ",")
            df = pd.read_csv(io.StringIO(text_content), sep=separator)

            logger.info(f"Loaded {len(df)} rows from CSV API response")
            return df

        except requests.RequestException as e:
            logger.error(f"API CSV request failed: {e}")
            raise

    @staticmethod
    def _is_gzip_content(content: bytes) -> bool:
        return content[:2] == b'\x1f\x8b'

    def _fetch_paginated(
        self,
        url: str,
        headers: dict,
        base_params: dict,
        config: dict
    ) -> list[dict]:
        all_data = []
        page_size = config.get("page_size", self.DEFAULT_PAGE_SIZE)
        max_pages = config.get("max_pages", self.DEFAULT_MAX_PAGES)
        page_param = config.get("page_param", "page")
        size_param = config.get("size_param", "perpage")
        data_path = config.get("data_path", "data.data")
        total_pages_path = config.get("total_pages_path", "data.total_pages")

        total_pages = None
        page = 1

        while page <= (total_pages or max_pages):
            params = {**base_params, page_param: page, size_param: page_size}

            try:
                response = requests.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=self.DEFAULT_TIMEOUT,
                    verify=False
                )
                response.raise_for_status()
                response_json = response.json()

                if total_pages is None:
                    total_pages = self._extract_nested(response_json, total_pages_path) or max_pages
                    logger.info(f"Total pages: {total_pages}")

                page_data = self._extract_nested(response_json, data_path)

                if not page_data:
                    if isinstance(response_json, list):
                        page_data = response_json
                    else:
                        break

                if isinstance(page_data, list):
                    all_data.extend(page_data)
                    logger.info(f"Page {page}: {len(page_data)} records")
                else:
                    break

                if len(page_data) < page_size:
                    break

                page += 1

            except requests.RequestException as e:
                logger.error(f"API request failed on page {page}: {e}")
                raise

        logger.info(f"Fetched {len(all_data)} records from {url}")
        return all_data

    @staticmethod
    def _extract_nested(data: dict, path: str):
        keys = path.split(".")
        result = data
        for key in keys:
            if isinstance(result, dict) and key in result:
                result = result[key]
            else:
                return None
        return result
