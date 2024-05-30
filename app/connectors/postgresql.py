from sqlalchemy import create_engine, Table, MetaData, inspect, Column
from sqlalchemy.engine import URL, CursorResult
from sqlalchemy.dialects import postgresql


class PostgreSqlClient:
    """
    A client for querying postgresql database.
    """

    def __init__(
        self,
        server_name: str,
        database_name: str,
        username: str,
        password: str,
        port: int = 5432,
    ):
        self.host_name = server_name
        self.database_name = database_name
        self.username = username
        self.password = password
        self.port = port

        connection_url = URL.create(
            drivername="postgresql+pg8000",
            username=username,
            password=password,
            host=server_name,
            port=port,
            database=database_name,
        )

        self.engine = create_engine(connection_url)

    def execute_sql(self, sql: str) -> None:
        self.engine.execute(sql)

    def select_all(self, table: Table) -> list[dict]:
        """
        Execute SQL code provided and returns the result in a list of dictionaries.
        This method should only be used if you expect a resultset to be returned.
        """
        return [dict(row) for row in self.engine.execute(table.select()).all()]

    def run_sql(self, sql: str) -> list[dict]:
        """
        Execute SQL code provided and returns the result in a list of dictionaries.
        This method should only be used if you expect a resultset to be returned.
        """
        return [dict(row) for row in self.engine.execute(sql).all()]

    def get_metadata(self) -> MetaData:
        """
        Gets the metadata object for all tables for a given database
        """
        metadata = MetaData(bind=self.engine)
        metadata.reflect()
        return metadata

    def get_table_schema(self, table_name: str) -> Table:
        """
        Gets the table schema and metadata
        """
        metadata = self.get_metadata()
        return metadata.tables[table_name], metadata

    def table_exists(self, table_name: str) -> bool:
        """
        Checks if the table already exists in the database.
        """
        return inspect(self.engine).has_table(table_name)

    def create_table(self, table_name: str, metadata: MetaData) -> None:
        """
        Creates a single table provided in the metadata object
        """
        existing_table = metadata.tables[table_name]
        new_metadata = MetaData()
        columns = [
            Column(column.name, column.type, primary_key=column.primary_key)
            for column in existing_table.columns
        ]
        new_table = Table(table_name, new_metadata, *columns)
        new_metadata.create_all(bind=self.engine)

    def create_all_tables(self, metadata: MetaData) -> None:
        """
        Creates tables provided in the metadata object
        """
        metadata.create_all(self.engine)

    def drop_table(self, table_name: str) -> None:
        """
        Drops a specified table if it exists
        """
        self.engine.execute(f"drop table if exists {table_name};")

    def insert(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        """
        Insert data into a database table. This method creates the table also if it doesn't exist.
        """
        self.create_table(table_name=table.name, metadata=metadata)
        insert_statement = postgresql.insert(table).values(data)
        self.engine.execute(insert_statement)

    def overwrite(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        """
        Overwrites data into a database table. This method creates the table also if it doesn't exist.
        """
        self.drop_table(table.name)
        self.insert(data=data, table=table, metadata=metadata)

    def upsert(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        """
        Upserts data into a database table. This method creates the table also if it doesn't exist.
        """
        self.create_table(table_name=table.name, metadata=metadata)
        key_columns = [
            pk_column.name for pk_column in table.primary_key.columns.values()
        ]
        insert_statement = postgresql.insert(table).values(data)
        upsert_statement = insert_statement.on_conflict_do_update(
            index_elements=key_columns,
            set_={
                c.key: c for c in insert_statement.excluded if c.key not in key_columns
            },
        )
        self.engine.execute(upsert_statement)

    def upsert_in_chunks(
        self, data: list[dict], table: Table, metadata: MetaData, chunksize: int = 1000
    ) -> None:
        """
        Upserts data into a database table in chunks (e.g. 1000 rows at a time) in case of query timeouts or row limitations.
        This method creates the table also if it doesn't exist.
        """
        max_length = len(data)
        for i in range(0, max_length, chunksize):
            if i + chunksize >= max_length:
                lower_bound = i
                upper_bound = max_length
            else:
                lower_bound = i
                upper_bound = i + chunksize
            self.upsert(
                data=data[lower_bound:upper_bound], table=table, metadata=metadata
            )
