import io
import json
import logging
import re

import pandas as pd

from src.context.commissions.domain.repositories import DataRepository
from src.context.commissions.domain.value_objects import DataSourceConfig

logger = logging.getLogger(__name__)

SCANIA_PRIMARY_DATA_PATTERNS = [
    "Datos por RUT",
    "Datos por Branch",
    "Cumplimiento",
]

RUT_BASED_ARRAY_NAMES = [
    "LeadTime",
    "Productividad",
    "Margenes",
    "Ausentismo",
    "Meta Plan de visitas",
    "Distribucion de Sucursales",
]

MERGE_KEY_CANDIDATES = [
    "branchid", "branch_id", "rut", "site", "sitio", "branch",
]

DETAIL_ARRAYS_NO_MERGE = [
    "Venta de contratos",
    "Visitas de Asesores",
    "contratos",
]


class S3DataRepository(DataRepository):

    def __init__(self, boto3_client=None):
        self.s3_client = boto3_client
        if self.s3_client is None:
            try:
                import boto3
                self.s3_client = boto3.client('s3')
            except ImportError:
                logger.warning("boto3 not installed. S3 data source will not work.")

    def get_data_for_plan(
        self,
        source: DataSourceConfig
    ) -> pd.DataFrame | dict[str, pd.DataFrame]:
        config = source.config

        bucket = config.get('bucket')
        separator = config.get('separator', ',')
        encoding = config.get('encoding', 'utf-8')

        if 'key' in config:
            key = config['key']
            return self._fetch_single_file(bucket, key, separator, encoding)

        elif 'prefix' in config and 'pattern' in config:
            prefix = config['prefix']
            pattern = config['pattern']
            max_files = config.get('max_files')

            if max_files and max_files > 1:
                current_key = config.get('current_key', 'current')
                historical_prefix = config.get('historical_key_prefix', 'historical')
                sort_order = config.get('sort_order', 'desc')

                return self._fetch_multiple_matching_files(
                    bucket=bucket,
                    prefix=prefix,
                    pattern=pattern,
                    max_files=max_files,
                    current_key=current_key,
                    historical_key_prefix=historical_prefix,
                    sort_order=sort_order,
                    separator=separator,
                    encoding=encoding
                )

            return self._fetch_latest_matching_file(bucket, prefix, pattern, separator, encoding)

        else:
            raise ValueError("S3 configuration must include either 'key' or both 'prefix' and 'pattern'")

    def _fetch_single_file(
        self,
        bucket: str,
        key: str,
        separator: str = ',',
        encoding: str = 'utf-8'
    ) -> pd.DataFrame:
        if not bucket or not key:
            raise ValueError("S3 configuration must include 'bucket' and 'key'")

        try:
            logger.info(f"Fetching data from S3: s3://{bucket}/{key}")

            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read()

            df = self._parse_content(content, key, separator, encoding)
            logger.info(f"Loaded {len(df)} rows from S3")
            return df

        except Exception as e:
            logger.error(f"Error reading from S3 s3://{bucket}/{key}: {str(e)}")
            raise

    def _fetch_latest_matching_file(
        self,
        bucket: str,
        prefix: str,
        pattern: str,
        separator: str = ',',
        encoding: str = 'utf-8'
    ) -> pd.DataFrame:
        try:
            matching_files = self._list_matching_files(bucket, prefix, pattern)

            if not matching_files:
                raise ValueError(f"No files matching pattern '{pattern}' found in s3://{bucket}/{prefix}")

            latest_file = self._select_latest_file(matching_files)

            logger.info(
                f"Found {len(matching_files)} matching files, using latest: "
                f"{latest_file['key']} (modified: {latest_file['last_modified']})"
            )

            return self._fetch_single_file(bucket, latest_file['key'], separator, encoding)

        except Exception as e:
            logger.error(f"Error listing/fetching files from S3 s3://{bucket}/{prefix}: {str(e)}")
            raise

    def _select_latest_file(self, matching_files: list[dict]) -> dict:
        import re
        period_pattern = re.compile(r'(\d{6})')

        def extract_period(file_info):
            filename = file_info['key'].split('/')[-1]
            match = period_pattern.search(filename)
            if match:
                return match.group(1)
            return '000000'

        sorted_by_period = sorted(matching_files, key=extract_period, reverse=True)

        if sorted_by_period:
            return sorted_by_period[0]

        return max(matching_files, key=lambda x: x['last_modified'])

    def _fetch_multiple_matching_files(
        self,
        bucket: str,
        prefix: str,
        pattern: str,
        max_files: int,
        current_key: str,
        historical_key_prefix: str,
        sort_order: str = 'desc',
        separator: str = ',',
        encoding: str = 'utf-8'
    ) -> dict[str, pd.DataFrame]:
        try:
            matching_files = self._list_matching_files(bucket, prefix, pattern)

            if not matching_files:
                raise ValueError(f"No files matching pattern '{pattern}' found in s3://{bucket}/{prefix}")

            reverse = sort_order == 'desc'
            matching_files.sort(key=lambda x: x['key'].split('/')[-1], reverse=reverse)

            files_to_process = matching_files[:max_files]

            logger.info(
                f"Found {len(matching_files)} matching files, processing {len(files_to_process)} "
                f"(sort: {sort_order})"
            )

            result = {}

            for idx, file_info in enumerate(files_to_process):
                df = self._fetch_single_file(
                    bucket, file_info['key'], separator, encoding
                )

                if idx == 0:
                    result[current_key] = df
                    logger.info(f"  [{current_key}]: {file_info['key']} ({len(df)} rows)")
                else:
                    key = f"{historical_key_prefix}_{idx}"
                    result[key] = df
                    logger.info(f"  [{key}]: {file_info['key']} ({len(df)} rows)")

            return result

        except Exception as e:
            logger.error(f"Error fetching multiple files from S3 s3://{bucket}/{prefix}: {str(e)}")
            raise

    def _list_matching_files(
        self,
        bucket: str,
        prefix: str,
        pattern: str
    ) -> list[dict]:
        logger.info(f"Listing files in s3://{bucket}/{prefix} matching pattern '{pattern}'")

        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        matching_files = []
        pattern_regex = re.compile(pattern.replace('*', '.*'))

        for page in pages:
            if 'Contents' not in page:
                continue

            for obj in page['Contents']:
                file_key = obj['Key']
                filename = file_key.split('/')[-1]

                if pattern_regex.match(filename):
                    matching_files.append({
                        'key': file_key,
                        'last_modified': obj['LastModified'],
                        'size': obj['Size']
                    })

        return matching_files

    @staticmethod
    def _parse_content(
        content: bytes,
        key: str,
        separator: str = ',',
        encoding: str = 'utf-8'
    ) -> pd.DataFrame:
        encodings_to_try = [encoding, 'utf-8', 'utf-8-sig', 'latin1', 'iso-8859-1']

        if key.endswith('.parquet'):
            return pd.read_parquet(io.BytesIO(content))

        elif key.endswith('.json'):
            for enc in encodings_to_try:
                try:
                    decoded_content = content.decode(enc)
                    return S3DataRepository._parse_json_content(decoded_content)
                except UnicodeDecodeError:
                    continue

            logger.warning(f"Could not decode {key} with standard encodings, using 'replace'")
            decoded_content = content.decode('utf-8', errors='replace')
            return S3DataRepository._parse_json_content(decoded_content)

        else:
            for enc in encodings_to_try:
                try:
                    return pd.read_csv(
                        io.BytesIO(content),
                        sep=separator,
                        encoding=enc
                    )
                except UnicodeDecodeError:
                    continue

            logger.warning(f"Could not decode {key} with standard encodings, using 'replace'")
            return pd.read_csv(
                io.BytesIO(content),
                sep=separator,
                encoding='utf-8',
                encoding_errors='replace'
            )

    @staticmethod
    def _parse_json_content(content: str) -> pd.DataFrame:
        data = json.loads(content)

        if data is None or data == {} or data == []:
            raise ValueError("S3 JSON file is empty or contains no data")

        if isinstance(data, list):
            if len(data) == 0:
                raise ValueError("S3 JSON file contains empty array")
            return pd.DataFrame(data)

        if isinstance(data, dict):
            arrays = S3DataRepository._find_all_data_arrays(data)
            if not arrays:
                if len(data) <= 2:
                    raise ValueError("S3 JSON file contains no data arrays (only metadata)")
            return S3DataRepository._extract_and_merge_nested_arrays(data)

        return pd.read_json(io.StringIO(content))

    @staticmethod
    def _extract_and_merge_nested_arrays(data: dict) -> pd.DataFrame:
        arrays = S3DataRepository._find_all_data_arrays(data)

        if not arrays:
            return pd.DataFrame([data])

        primary_df = S3DataRepository._get_primary_dataframe(arrays)
        primary_key = None
        if primary_df is None or primary_df.empty:
            first_key = next(iter(arrays))
            primary_df = pd.DataFrame(arrays[first_key])
            primary_key = first_key
            logger.info(f"Using '{first_key}' as primary data ({len(primary_df)} records)")
        else:
            for key, records in arrays.items():
                if pd.DataFrame(records).equals(primary_df):
                    primary_key = key
                    break

        secondary_arrays = {}
        for key, records in arrays.items():
            if key == primary_key:
                continue
            secondary_df = pd.DataFrame(records)
            if secondary_df.empty:
                continue

            is_detail_array = any(pattern in key for pattern in DETAIL_ARRAYS_NO_MERGE)

            if is_detail_array:
                secondary_arrays[key] = secondary_df
                logger.info(f"Stored '{key}' ({len(secondary_df)} rows) as detail array (no merge)")
                continue

            merged = S3DataRepository._merge_dataframes(primary_df, secondary_df, key)
            if len(merged.columns) > len(primary_df.columns):
                primary_df = merged
            else:
                secondary_arrays[key] = secondary_df
                logger.info(f"Stored '{key}' ({len(secondary_df)} rows) as secondary array for later merge")

        if secondary_arrays:
            primary_df.attrs['secondary_arrays'] = secondary_arrays

        return primary_df

    @staticmethod
    def _find_all_data_arrays(data: dict) -> dict[str, list]:
        arrays = {}
        for key, value in data.items():
            if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                arrays[key] = value
        return arrays

    @staticmethod
    def _get_primary_dataframe(arrays: dict[str, list]) -> pd.DataFrame | None:
        for pattern in SCANIA_PRIMARY_DATA_PATTERNS:
            for key, records in arrays.items():
                if pattern in key:
                    logger.info(f"Found primary data in '{key}' ({len(records)} records)")
                    return pd.DataFrame(records)

        for rut_key in RUT_BASED_ARRAY_NAMES:
            for key, records in arrays.items():
                if rut_key in key and len(records) > 0:
                    df = pd.DataFrame(records)
                    if "RUT" in df.columns or "rut" in df.columns:
                        logger.info(f"Found RUT-based primary data in '{key}' ({len(records)} records)")
                        return df

        def has_branchid_column(records: list) -> bool:
            all_keys = set()
            for rec in records[:10]:
                all_keys.update(k.lower() for k in rec.keys())
            return "branchid" in all_keys

        arrays_with_branchid = [
            (key, records) for key, records in arrays.items()
            if records and has_branchid_column(records)
        ]
        if arrays_with_branchid:
            key, records = max(arrays_with_branchid, key=lambda x: len(x[1]))
            logger.info(f"Using BranchID-based array '{key}' as primary ({len(records)} records)")
            return pd.DataFrame(records)

        return None

    @staticmethod
    def _merge_dataframes(primary: pd.DataFrame, secondary: pd.DataFrame, key: str) -> pd.DataFrame:
        primary_cols = [c.lower() for c in primary.columns]
        secondary_cols = [c.lower() for c in secondary.columns]

        merge_key = None
        for candidate in MERGE_KEY_CANDIDATES:
            if candidate in primary_cols and candidate in secondary_cols:
                merge_key = candidate
                break

        if merge_key is None:
            if len(secondary) == 1:
                logger.debug(f"No merge key for '{key}', broadcasting single row to all")
                for col in secondary.columns:
                    col_lower = col.lower()
                    if col_lower not in primary_cols:
                        primary[col] = secondary[col].iloc[0]
            else:
                logger.debug(f"No merge key for '{key}' with {len(secondary)} rows, skipping merge")
            return primary

        primary_temp = primary.copy()
        secondary_temp = secondary.copy()
        primary_temp.columns = primary_temp.columns.str.lower()
        secondary_temp.columns = secondary_temp.columns.str.lower()

        new_cols = [c for c in secondary_temp.columns if c not in primary_temp.columns and c != merge_key]
        if not new_cols:
            return primary

        merge_cols = [merge_key] + new_cols
        secondary_subset = secondary_temp[merge_cols].drop_duplicates(subset=[merge_key])

        result = primary_temp.merge(secondary_subset, on=merge_key, how="left", suffixes=("", f"_{key[:10]}"))
        result.columns = primary.columns.tolist() + new_cols
        logger.debug(f"Merged '{key}' on '{merge_key}': added {len(new_cols)} columns")
        return result
