import pandas as pd
from utils.config import get_config
from utils.logger import get_logger

logger = get_logger(__name__)

class BatchDataIngestor:
    """
    Handles batch data ingestion from various sources.
    """

    def __init__(self, config_path="config.yaml"):
        self.config = get_config(config_path)
        self.data_sources = self.config.get("data_sources", {})

    def ingest_data(self, source_name):
        """
        Ingests batch data from a specified source.

        Args:
            source_name (str): Name of the data source as defined in the configuration.

        Returns:
            pandas.DataFrame: Ingested data as a Pandas DataFrame.
        """
        if source_name not in self.data_sources:
            raise ValueError(f"Invalid data source name: {source_name}")

        source_config = self.data_sources[source_name]
        source_type = source_config.get("type")

        logger.info(f"Ingesting batch data from source: {source_name} ({source_type})")

        if source_type == "csv":
            data = self._ingest_csv(source_config)
        elif source_type == "parquet":
            data = self._ingest_parquet(source_config)
        elif source_type == "database":
            data = self._ingest_database(source_config)
        else:
            raise ValueError(f"Unsupported data source type: {source_type}")

        logger.info(f"Successfully ingested data from {source_name}")
        return data

    def _ingest_csv(self, source_config):
        """
        Ingests data from a CSV file.
        """
        filepath = source_config.get("filepath")
        delimiter = source_config.get("delimiter", ",")
        return pd.read_csv(filepath, delimiter=delimiter)

    def _ingest_parquet(self, source_config):
        """
        Ingests data from a Parquet file.
        """
        filepath = source_config.get("filepath")
        return pd.read_parquet(filepath)

    def _ingest_database(self, source_config):
        """
        Ingests data from a database.
        """
        conn_str = source_config.get("connection_string")
        query = source_config.get("query")

        if not conn_str or not query:
            raise ValueError(
                "Connection string and query are required for database ingestion."
            )

        # database connection and query execution

        data = pd.read_sql(query, conn_str)

        # return data
        raise NotImplementedError("Database ingestion is not yet implemented.")

