from pyspark.sql import DataFrameWriter


class PostgresConnector(object):
    def __init__(self, hostname, dbname, username, password):
        self.db_name = dbname
        self.hostname = hostname
        self.url_connect = "jdbc:postgresql://{hostname}:5432/{db}".format(
            hostname=self.hostname, db=self.db_name)
        self.properties = {
            "user": username,
            "password": password,
            "driver": "org.postgresql.Driver"
        }

    def write(self, df, table, md):
        """
        Args:
            df: pandas.dataframe
            table: str
            md: str 
        :rtype: None
        """
        writer = DataFrameWriter(df)
        writer.jdbc(self.url_connect, table, md, self.properties)
