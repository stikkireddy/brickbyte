import os
from pathlib import Path

from databricks import sql


class DatabricksSqlClient:
    def __init__(self,
                 server_hostname: str,
                 http_path: str,
                 token: str,
                 catalog: str,
                 schema: str,
                 staging_volume_path: str = None,
                 local_temp_dir: str = None
                 ):
        self.server_hostname = server_hostname
        self.http_path = http_path
        self.token = token
        self.catalog = catalog
        self.schema = schema
        self.staging_volume_path = staging_volume_path
        if self.staging_volume_path is not None:
            self.local_stage_dir = local_temp_dir or os.path.join(Path.home(), "local_brickbyte_stage")
            os.makedirs(self.local_stage_dir, exist_ok=True)
        else:
            self.local_stage_dir = None

    def open(self):
        connection = sql.connect(
            server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME", self.server_hostname),
            http_path=os.getenv("DATABRICKS_HTTP_PATH", self.http_path),
            access_token=os.getenv("DATABRICKS_TOKEN", self.token),
            catalog=self.catalog,
            schema=self.schema,
            staging_allowed_local_path=self.local_stage_dir
        )

        return connection.cursor()
