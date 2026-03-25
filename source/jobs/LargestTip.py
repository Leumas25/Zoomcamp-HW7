from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def create_events_aggregarted_sink(t_env):
    table_name = 'largest_tip'
    sink_ddl = f"""
    CREATE TABLE {table_name} (
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        total_tip DOUBLE NOT NULL,
        PRIMARY KEY (window_start, window_end) NOT ENFORCED
    ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver',
            'sink.buffer-flush.max-rows' = '50',
            'sink.buffer-flush.interval' = '2s'
    );
    """
    t_env.execute_sql(sink_ddl)
    return table_name

def create_events_source_kafka(t_env):
    table_name = "events"
    source_ddl = f"""
    CREATE TABLE {table_name} (
        PULocationID INTEGER,
        DOLocationID INTEGER,
        trip_distance DOUBLE,
        total_amount DOUBLE,
        tip_amount DOUBLE,
        lpep_pickup_datetime BIGINT,
        event_timestamp AS TO_TIMESTAMP_LTZ(lpep_pickup_datetime, 3),
        WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' seconds
    ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'green_trips',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
    );
    """
    t_env.execute_sql(source_ddl)
    return table_name

def log_processing():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    # Finite Kafka batches stop emitting records; without this, idle partitions block the
    # watermark and the last tumbling windows never finish, so JDBC never sees rows.
    t_env.get_config().get_configuration().set_string("table.exec.source.idle-timeout", "5 s")
    try:
        source_table = create_events_source_kafka(t_env)
        sink_table = create_events_aggregarted_sink(t_env)
        t_env.execute_sql(
            f"""
            INSERT INTO {sink_table}
            SELECT
                window_start,
                window_end,
                SUM(tip_amount) AS total_tip
            FROM TABLE (
                TUMBLE(
                    TABLE {source_table},
                    DESCRIPTOR(event_timestamp),
                    INTERVAL '1' HOUR
                )
            )
            GROUP BY window_start, window_end
            """
        ).wait()
    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))
        
        
if __name__ == "__main__":
    log_processing()