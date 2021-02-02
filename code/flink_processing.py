from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

# Setup the Flink Streaming Engine
def flink_processing():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env_settings = EnvironmentSettings.Builder().use_blink_planner().build()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env,
                                          environment_settings=env_settings)

    t_env.get_config().get_configuration().set_string(
        "pipeline.jars",
        "file:////D:/temp/kafka_2.12-2.7.0/flink-connector-kafka_2.11-1.12.0.jar;"
        "file:////D:/temp/kafka_2.12-2.7.0/flink-sql-connector-kafka_2.11-1.12.0.jar"
    )
    # create dummy functionality on writing data from Kafka in a table and then in another table
    source_ddl = """
                    CREATE TABLE source_num(
                      `ts` TIMESTAMP(3) METADATA FROM 'timestamp',
                      `step` FLOAT,
                      `edge_id` STRING,
                      `vehicle_num` INT
                    ) WITH (
                      'connector' = 'kafka',
                      'topic' = 'source_num',
                      'properties.bootstrap.servers' = 'localhost:9092',
                      'properties.group.id' = 'new_group2',
                      'format' = 'json'
                    )
                    """

    sink_ddl = """
                    CREATE TABLE sink_table_num(
                        `ts` TIMESTAMP(3) METADATA FROM 'timestamp',
                        `step` FLOAT,
                        `edge_id` STRING,
                        `vehicle_num` INT
                    ) WITH (
                      'connector' = 'kafka',
                      'topic' = 'sink_topic_num',
                      'properties.bootstrap.servers' = 'localhost:9092',
                      'format' = 'json'
                    )
                    """
    # Execute the queries
    t_env.execute_sql(source_ddl)
    t_env.execute_sql(sink_ddl)
    # The dummy stream processing query executed in Flink
    # select from table and insert in the sink table
    t_env.sql_query(
        "SELECT `ts`, `step`, `edge_id`, `vehicle_num` "
        "FROM `source_num` "
        "WHERE `edge_id`='313576543#2'"  # Dachauerstrasse - Lothstrasse
    ).execute_insert("sink_table_num").wait()


if __name__ == "__main__":
    flink_processing()
