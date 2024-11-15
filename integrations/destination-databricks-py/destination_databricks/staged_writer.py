import json
import logging
import os
import tempfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from airbyte_cdk.models import AirbyteConnectionStatus, Status
from virtualenv.create.via_global_ref.builtin.ref import PathRef

from destination_databricks.client import DatabricksSqlClient


class DatabricksSqlStagedWriter:
    """
    Base class for shared writer logic.
    """

    flush_interval = 250000

    def __init__(self, client: DatabricksSqlClient) -> None:
        """
        :param client: Databricks SDK connection class with established connection
            to the databse.
        """
        try:
            # open a cursor and do some work with it
            self.client = client
            self._buffer = defaultdict(list)
            self._values = 0
        except Exception as e:
            # handle the exception
            raise AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {repr(e)}")

        if self.client.staging_volume_path is None:
            raise AirbyteConnectionStatus(status=Status.FAILED, message="Staging volume path is not provided")

    def delete_table(self, name: str) -> None:
        """
        Delete the resulting table.
        Primarily used in Overwrite strategy to clean up previous data.

        :param name: table name to delete.
        """
        cursor = self.client.open()
        cursor.execute(f"DROP TABLE IF EXISTS _airbyte_raw_{name}")
        cursor.close()

    def create_raw_table(self, name: str):
        """
        Create the resulting _airbyte_raw table.

        :param name: table name to create.
        """
        query = f"""
        CREATE TABLE IF NOT EXISTS _airbyte_raw_{name} (
            _airbyte_ab_id STRING,
            _airbyte_emitted_at TIMESTAMP,
            _airbyte_data STRING
        )
        """
        cursor = self.client.open()
        cursor.execute(query)
        cursor.close()

    def queue_write_data(self, stream_name: str, id: str, time: datetime, record: str) -> None:
        """
        Queue up data in a buffer in memory before writing to the database.
        When flush_interval is reached data is persisted.

        :param stream_name: name of the stream for which the data corresponds.
        :param id: unique identifier of this data row.
        :param time: time of writing.
        :param record: string representation of the json data payload.
        """
        self._buffer[stream_name].append((id, time, record))
        self._values += 1
        if self._values == self.flush_interval:
            self._flush()

    def _flush(self):
        """
        Stub for the intermediate data flush that's triggered during the
        buffering operation.
        """
        raise NotImplementedError()

    def flush(self):
        """
        Stub for the data flush at the end of writing operation.
        """
        raise NotImplementedError()


class DatabricksSqlStagedWriterImpl(DatabricksSqlStagedWriter):
    """
    Data writer using the SQL writing strategy. Data is buffered in memory
    and flushed using INSERT INTO SQL statement.
    """

    flush_interval = 1000

    def __init__(self, client: DatabricksSqlClient) -> None:
        """
        :param client: Databricks SDK connection class with established connection
            to the databse.
        """
        super().__init__(client)

    @staticmethod
    def quote_value(value):
        if isinstance(value, str):
            res = value.replace('\'', '\'\'')
            return f"'{res}'"  # Escape single quotes in strings
        elif isinstance(value, dict):
            res = json.dumps(value).replace('\'', '\'\'')
            return f"'{res}'"  # Convert dict to JSON and escape
        elif value is None:
            return "NULL"  # Handle NULLs
        elif isinstance(value, datetime):
            return f"'{str(value)}'"
        else:
            return str(value)  # For numbers and other types

    def _flush(self) -> None:
        """
        Intermediate data flush that's triggered during the
        buffering operation. Writes data stored in memory via SQL commands.
        Databricks connector insert into table using stage
        """
        cursor = self.client.open()

        # stage the data in a jsonl file using TemporaryFile
        # then load the data into the table
        with tempfile.NamedTemporaryFile(mode='w+', dir=self.client.local_stage_dir) as temp_file:
            for table, data in self._buffer.items():
                for idx, (id, time, record) in enumerate(data):
                    temp_file.write(json.dumps({
                        "_airbyte_ab_id": id,
                        "_airbyte_emitted_at": str(time),
                        "_airbyte_data": record
                    }))
                    if idx != len(data) - 1:
                        temp_file.write('\n')
                temp_file.flush()
                temp_file.seek(0)
                temp_file_absolute_path = temp_file.name
                this_batch_id = str(datetime.now().strftime("%Y%m%d%H%M%S"))
                stage_path = os.path.join(self.client.staging_volume_path,
                                          f"_airbyte_raw_{table}",
                                          this_batch_id,
                                          "data.jsonl")
                put_statement = f"PUT '{temp_file_absolute_path}' INTO '{stage_path}' OVERWRITE"
                copy_into_statement = f"COPY INTO _airbyte_raw_{table} FROM '{stage_path}' FILEFORMAT = JSON"
                cleanup_statement = f"REMOVE '{stage_path}'"
                print("Running", put_statement)
                cursor.execute(put_statement)
                print("Running", copy_into_statement)
                cursor.execute(copy_into_statement)
                print("Running", cleanup_statement)
                cursor.execute(cleanup_statement)
        self._buffer.clear()
        self._values = 0
        cursor.close()

    def flush(self) -> None:
        """
        Final data flush after all data has been written to memory.
        """
        self._flush()


def create_databricks_staged_writer(client: DatabricksSqlClient, logger: logging.Logger) -> DatabricksSqlStagedWriter:
    logger.info("Using the SQL writing strategy")
    writer = DatabricksSqlStagedWriterImpl(client)
    return writer
