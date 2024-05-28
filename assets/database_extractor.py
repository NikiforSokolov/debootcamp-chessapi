from jinja2 import Environment
from etl_project.connectors.postgresql import PostgreSqlClient
from pathlib import Path
from sqlalchemy import Table, MetaData
import logging


class SqlExtractConfig:
    FULL_EXTRACT = "full"
    INCREMENTAL_EXTRACT = "incremental"
    EXTRACT_TYPES = [FULL_EXTRACT, INCREMENTAL_EXTRACT]

    def __init__(
        self,
        source_table_name: str,
        extract_type: str = FULL_EXTRACT,
        incremental_column: str = None,
    ):
        if extract_type not in SqlExtractConfig.EXTRACT_TYPES:
            raise Exception(
                f"Extract type '{extract_type}' is not supported. Please choose from {SqlExtractConfig.EXTRACT_TYPES}."
            )
        if source_table_name is None:
            raise Exception(
                f"Please specify a source_table_name in your asset's config block."
            )
        self.source_table_name = source_table_name
        self.extract_type = extract_type
        self.incremental_column = incremental_column


class SqlExtractParser:
    def __init__(self, file_path: Path, environment: Environment):
        self.file_path = file_path
        self.environment = environment
        self.template = self.environment.get_template(self.file_path)
        self.config = SqlExtractConfig(**self.template.make_module().config)

    def get_templated_sql(self, **kwargs) -> str:
        """
        This method accepts the same arguments as the dict constructor: A dict, a dict subclass or some keyword arguments. If no arguments are given the context will be empty. These two calls do the same:

                ```python
                sql_extract_parser.get_templated_sql(knights='that say nih')
                sql_extract_parser.get_templated_sql({'knights': 'that say nih'})
                ```

        This will return the rendered template as a string.
        """
        return self.template.render(**kwargs)

    def get_config(self) -> SqlExtractConfig:
        return self.config


class DatabaseTableExtractor:
    def __init__(
        self,
        sql_extract_parser: SqlExtractParser,
        source_postgresql_client: PostgreSqlClient,
        target_postgresql_client: PostgreSqlClient,
    ):
        self.sql_extract_parser = sql_extract_parser
        self.source_postgresql_client = source_postgresql_client
        self.target_postgresql_client = target_postgresql_client

    def _full_extract(self) -> list[dict]:
        return self.source_postgresql_client.run_sql(
            self.sql_extract_parser.get_templated_sql(is_incremental=False)
        )

    def _get_incremental_value(self) -> str:
        sql = f"""
            select max({self.sql_extract_parser.config.incremental_column}) as incremental_value
            from {self.sql_extract_parser.config.source_table_name}
        """
        sql_response = self.target_postgresql_client.run_sql(sql)
        return sql_response[0].get("incremental_value")

    def _incremental_extract(self) -> list[dict]:
        if self.target_postgresql_client.table_exists(
            self.sql_extract_parser.config.source_table_name
        ):
            incremental_value = self._get_incremental_value()
            templated_sql = self.sql_extract_parser.get_templated_sql(
                is_incremental=True, incremental_value=incremental_value
            )
            return self.source_postgresql_client.run_sql(templated_sql)
        else:
            logging.info(
                f"Table '{self.sql_extract_parser.config.source_table_name}' does not exist. Performing full extract."
            )
            return self._full_extract()

    def extract(self) -> list[dict]:
        """
        Performs database table extraction using either a full extract or incremental extract pattern.
        The extraction method used will depend on the SqlExtractParser instance passed to the constructor.
        """
        if self.sql_extract_parser.config.extract_type == SqlExtractConfig.FULL_EXTRACT:
            return self._full_extract()
        elif (
            self.sql_extract_parser.config.extract_type
            == SqlExtractConfig.INCREMENTAL_EXTRACT
        ):
            return self._incremental_extract()
        else:
            raise Exception(
                f"Extraction type '{self.sql_extract_parser.config.extract_type}' is not supported. Skipping extraction."
            )

    def get_table_schema(self) -> tuple[Table, MetaData]:
        return self.source_postgresql_client.get_table_schema(
            table_name=self.sql_extract_parser.config.source_table_name
        )
