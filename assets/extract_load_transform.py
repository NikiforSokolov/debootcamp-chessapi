from jinja2 import Environment
from assets.database_extractor import (
    SqlExtractParser,
    DatabaseTableExtractor,
)
from connectors.postgresql import PostgreSqlClient
from graphlib import TopologicalSorter


def extract_load(
    template_environment: Environment,
    source_postgresql_client: PostgreSqlClient,
    target_postgresql_client: PostgreSqlClient,
):
    """
    Perform data extraction specified in a jinja template_environment.

    Data is extracted using a source_postgresql_client, and loaded using a target_postgresql_client.
    """
    for asset in template_environment.list_templates():
        sql_extract_parser = SqlExtractParser(
            file_path=asset, environment=template_environment
        )
        database_table_extractor = DatabaseTableExtractor(
            sql_extract_parser=sql_extract_parser,
            source_postgresql_client=source_postgresql_client,
            target_postgresql_client=target_postgresql_client,
        )
        table_schema, metadata = database_table_extractor.get_table_schema()
        table_data = database_table_extractor.extract()
        target_postgresql_client.upsert_in_chunks(
            data=table_data, table=table_schema, metadata=metadata
        )


class SqlTransform:
    def __init__(
        self,
        postgresql_client: PostgreSqlClient,
        environment: Environment,
        table_name: str,
    ):
        self.postgresql_client = postgresql_client
        self.environment = environment
        self.table_name = table_name
        self.template = self.environment.get_template(f"{table_name}.sql")

    def create_table_as(self) -> None:
        """
        Drops the table if it exists and creates a new copy of the table using the provided select statement.
        """
        exec_sql = f"""
            drop table if exists {self.table_name};
            create table {self.table_name} as (
                {self.template.render()}
            )
        """
        self.postgresql_client.execute_sql(exec_sql)


def transform(dag: TopologicalSorter):
    """
    Performs `create table as` on all nodes in the provided DAG.
    """
    dag_rendered = tuple(dag.static_order())
    for node in dag_rendered:
        node.create_table_as()
