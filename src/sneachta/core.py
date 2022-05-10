
"""A suite of commands for using Snowflake interactively in python."""

from getpass import getpass
from typing import Optional

import pandas as pd
import snowflake.connector
from snowflake.connector import OperationalError
from snowflake.connector.cursor import SnowflakeCursor

from sneachta.exceptions import (
    SnowflakeConnectionError, SnowflakeExecutionError
)


CHUNK_SIZE = 50_000


class SnowflakeClient(object):
    """
    An interface for the Snowflake Data Warehouse.

    Parameters
    ----------
    username : str
    role : str
    warehouse : str
    database : str
    password : str
    account : str
        Account identifier to connect to Snowflake.
    """

    def __init__(
        self,
        username: Optional[str] = None,
        role: Optional[str] = None,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        password: Optional[str] = None,
        account: Optional[str] = None,
        chunk_size: int = CHUNK_SIZE
    ) -> None:
        """Initialise the client with the necessary credentials."""
        super().__init__()
        self.username = username
        self.role = role
        self.warehouse = warehouse
        self.database = database
        self.password = password or getpass("Snowflake password:")
        self.account = account
        self.chunk_size = chunk_size

    def _connect(self):
        """Connect to the snowflake DB."""
        return snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            database=self.database,
            warehouse=self.warehouse,
            login_timeout=self.login_timeout,
            network_timeout=self.network_timeout
        )

    def get_cursor(self) -> SnowflakeCursor:
        """Retrieve a cursor."""
        # Connect to snowflake
        try:
            conn = self._connect()
        except OperationalError as e:
            raise SnowflakeConnectionError(e.args[0])
        return conn.cursor()

    def query(self, query: str) -> pd.DataFrame:
        """Submit a query and return the results in a Pandas DataFrame."""
        conn = self._connect()
        cursor = conn.cursor()
        chunks = []
        try:
            chunk_iter = pd.read_sql_query(
                query, conn, chunksize=self.chunk_size
            )

            for idx, chunk in enumerate(chunk_iter):
                chunks.append(chunk)

        except Exception as e:
            cursor.execute('rollback;')
            raise SnowflakeExecutionError(
                f'ROLLBACK: could not complete queries:\n{e}'
            )
        else:
            return pd.concat(chunks)

        finally:
            conn.close()

    def query_to_csv(self, query: str, output: str, **csv_params) -> None:
        """Submit a query and write the results to a CSV."""
        frame = self.query(query)
        kwargs = {
            "encoding": "utf-8",
            "index": False
        }
        if csv_params:
            kwargs.update(csv_params)

        frame.to_csv(output, **kwargs)

    def create_from_dataframe(self):
        """Create a table from contents of a Pandas DataFrame."""
        raise NotImplementedError()

    def insert_from_dataframe(self):
        """Insert the contents of a Pandas DataFrame into a table."""
        raise NotImplementedError()
