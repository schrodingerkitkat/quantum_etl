from time import sleep
from utils.config import get_config
from utils.logger import get_logger
from kafka import KafkaConsumer


logger = get_logger(__name__)


class StreamDataIngestor:
    """
    Handles real-time streaming data ingestion.
    """

    def __init__(self, config_path="config.yaml"):
        self.config = get_config(config_path)
        self.stream_sources = self.config.get("stream_sources", {})

    def ingest_stream(self, source_name):
        """
        Connects to a stream source and yields data in real-time.

        Args:
            source_name (str): Name of the stream source as defined in the configuration.

        Yields:
            dict: A dictionary representing a single record from the stream.
        """
        if source_name not in self.stream_sources:
            raise ValueError(f"Invalid stream source name: {source_name}")

        source_config = self.stream_sources[source_name]
        source_type = source_config.get("type")

        logger.info(
            f"Connecting to stream source: {source_name} ({source_type})"
        )

        if source_type == "kafka":
            yield from self._ingest_kafka(source_config)
        else:
            raise ValueError(f"Unsupported stream source type: {source_type}")

    def _ingest_kafka(self, source_config):
        """
        Connects to a Kafka topic and yields messages.
        """
        # Kafka consumer implementation
        # using kafka-python:
        consumer = KafkaConsumer(
             source_config.get("topic"),
             bootstrap_servers=source_config.get("bootstrap_servers"),
             **source_config.get("consumer_config", {})
         )

        # for message in consumer:
             yield message.value.decode('utf-8')
        while True:
            # Simulate receiving streaming data
            data = {"example": "data"}
            logger.info(f"Received stream data: {data}")
            yield data
            sleep(1)

