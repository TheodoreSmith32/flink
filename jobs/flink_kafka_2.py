import itertools
import os

from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

PREVIEW_LIMIT = 20  # jumlah pesan yang ditarik, lalu job dibatalkan otomatis

bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "").strip()
topic = os.environ.get("KAFKA_TOPIC", "").strip()

env = StreamExecutionEnvironment.get_execution_environment()

jar_path = os.path.join(os.path.abspath("."), "jars", "flink-sql-connector-kafka-1.17.2.jar")
env.add_jars(f"file://{jar_path}")

kafka_source = (
    KafkaSource.builder()
    .set_bootstrap_servers(bootstrap_servers)
    .set_topics(topic)
    .set_group_id("pyflink-belajar-ui")
    .set_starting_offsets(KafkaOffsetsInitializer.earliest())
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)

stream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "kafka_source")

# execute_and_collect() -> iterator di sisi client. Ambil PREVIEW_LIMIT baris
# lalu close() supaya job Flink di baliknya ikut dibatalkan -- kalau tidak,
# job Kafka-nya unbounded dan bakal ngegantung session ini selamanya.
iterator = stream.execute_and_collect()
try:
    rows = list(itertools.islice(iterator, PREVIEW_LIMIT))
finally:
    iterator.close()

print(f"Ambil {len(rows)} pesan dari topic '{topic}':")
for row in rows:
    print(row)