import os

from databricks import sql


class DatabricksSqlClient:
    def __init__(self,
                 server_hostname: str,
                 http_path: str,
                 token: str,
                 catalog: str,
                 schema: str,
                 staging_volume_path: str = None,
                 ):
        self.server_hostname = server_hostname
        self.http_path = http_path
        self.token = token
        self.catalog = catalog
        self.schema = schema
        self.staging_volume_path = staging_volume_path

    def open(self):
        connection = sql.connect(
            server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME", self.server_hostname),
            http_path=os.getenv("DATABRICKS_HTTP_PATH", self.http_path),
            access_token=os.getenv("DATABRICKS_TOKEN", self.token),
            catalog=self.catalog,
            schema=self.schema
        )

        return connection.cursor()
