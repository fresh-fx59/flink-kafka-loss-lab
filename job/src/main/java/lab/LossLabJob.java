package lab;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The job under test. Everything is driven by environment variables so a scenario is
 * reproducible from its env file alone - the job is never edited between runs.
 *
 * SHAPE=direct : kafka1 -> filter -> Postgres          (the operator's Type 2)
 * SHAPE=router : kafka1 -> filter -> kafka2            (the operator's Type 1, job R)
 * SHAPE=sink   : kafka2 -> Postgres                    (the operator's Type 1, job S)
 */
public class LossLabJob {

    private static final Logger LOG = LoggerFactory.getLogger(LossLabJob.class);

    private static String env(String key, String dflt) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? dflt : v;
    }

    private static long envLong(String key, long dflt) {
        return Long.parseLong(env(key, Long.toString(dflt)));
    }

    private static boolean envBool(String key, boolean dflt) {
        return Boolean.parseBoolean(env(key, Boolean.toString(dflt)));
    }

    /**
     * The whole point of the investigation lives in this method. Each branch is one of
     * the startup behaviours the vault note enumerates, and the scenario catalogue
     * exercises every one of them.
     */
    private static OffsetsInitializer startingOffsets(String spec) {
        if (spec.startsWith("pg-max:")) {
            // Resume from MAX(src_offset) in one table. Deliberately the wrong design -
            // a maximum is not a completed prefix. S12 exists to show it losing data.
            return new PgOffsetsInitializer(
                    env("PG_URL", "jdbc:postgresql://postgres:5432/lab"),
                    env("PG_USER", "lab"), env("PG_PASSWORD", "lab"),
                    PgOffsetsInitializer.Mode.MAX_OFFSET,
                    spec.substring("pg-max:".length()), env("JOB_NAME", "loss-lab"));
        }
        if (spec.equals("pg-frontier")) {
            // Resume from MIN over every branch's acknowledged progress. S13.
            return new PgOffsetsInitializer(
                    env("PG_URL", "jdbc:postgresql://postgres:5432/lab"),
                    env("PG_USER", "lab"), env("PG_PASSWORD", "lab"),
                    PgOffsetsInitializer.Mode.FRONTIER, null, env("JOB_NAME", "loss-lab"));
        }
        if (spec.startsWith("timestamp:")) {
            long ts = Long.parseLong(spec.substring("timestamp:".length()));
            // NOTE: Flink 1.17's TimestampOffsetsInitializer falls back to the
            // partition's END offset when the lookup returns nothing, and its reset
            // strategy is LATEST. A failed lookup therefore SKIPS that partition's
            // backlog silently. Scenario S22 exists to prove exactly this.
            return OffsetsInitializer.timestamp(ts);
        }
        switch (spec) {
            case "earliest":
                return OffsetsInitializer.earliest();
            case "latest":
                return OffsetsInitializer.latest();
            case "committed-earliest":
                return OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST);
            case "committed-latest":
                return OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST);
            case "committed-none":
                return OffsetsInitializer.committedOffsets();
            default:
                throw new IllegalArgumentException("unknown STARTING_OFFSETS: " + spec);
        }
    }

    private static KafkaSource<Event> buildSource(String bootstrap, String topic) {
        KafkaSourceBuilder<Event> b = KafkaSource.<Event>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId(env("GROUP_ID", "loss-lab"))
                .setStartingOffsets(startingOffsets(env("STARTING_OFFSETS", "committed-earliest")))
                .setDeserializer(new EventDeserializer());

        // KafkaSourceBuilder defaults enable.auto.commit to false. With checkpointing
        // off and this left alone, NOTHING is ever committed and committed-earliest
        // degenerates into earliest() on every restart. S01 vs S02 is that difference.
        b.setProperty("enable.auto.commit", env("ENABLE_AUTO_COMMIT", "false"));
        b.setProperty("auto.commit.interval.ms", env("AUTO_COMMIT_INTERVAL_MS", "5000"));
        b.setProperty("commit.offsets.on.checkpoint", env("COMMIT_OFFSETS_ON_CHECKPOINT", "true"));
        String discovery = env("PARTITION_DISCOVERY_INTERVAL_MS", "");
        if (!discovery.isEmpty()) {
            b.setProperty("partition.discovery.interval.ms", discovery);
        }
        return b.build();
    }

    /** Captures the Kafka coordinates, which several scenarios write into Postgres. */
    public static class EventDeserializer implements KafkaRecordDeserializationSchema<Event> {
        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Event> out) {
            if (record.value() == null) return;
            String value = new String(record.value(), java.nio.charset.StandardCharsets.UTF_8);
            out.collect(Event.parse(value, record.topic(), record.partition(), record.offset()));
        }

        @Override
        public TypeInformation<Event> getProducedType() {
            return TypeInformation.of(Event.class);
        }
    }

    public static void main(String[] args) throws Exception {
        final String shape = env("SHAPE", "direct");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Integer.parseInt(env("PARALLELISM", "2")));

        final long checkpointMs = envLong("CHECKPOINTING_MS", 0);
        if (checkpointMs > 0) {
            env.enableCheckpointing(checkpointMs, CheckpointingMode.EXACTLY_ONCE);
            CheckpointConfig cc = env.getCheckpointConfig();
            cc.setCheckpointStorage(env("CHECKPOINT_DIR", "file:///checkpoints"));
            cc.setMinPauseBetweenCheckpoints(Math.max(1000, checkpointMs / 4));
            cc.setCheckpointTimeout(300_000);
            cc.setMaxConcurrentCheckpoints(1);
            // Aligned on purpose: FLINK-31963 breaks unaligned-checkpoint rescaling on
            // 1.17.0 (fixed in 1.17.1). Never turn this on for this Flink version.
            cc.enableUnalignedCheckpoints(false);
            cc.setExternalizedCheckpointCleanup(
                    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
            LOG.info("checkpointing ENABLED every {} ms, dir={}", checkpointMs,
                    env("CHECKPOINT_DIR", "file:///checkpoints"));
        } else {
            LOG.info("checkpointing DISABLED - offsets move only if auto-commit is on");
        }

        final String k1 = env("KAFKA1_BOOTSTRAP", "kafka1-1:9092,kafka1-2:9092,kafka1-3:9092");
        final String k2 = env("KAFKA2_BOOTSTRAP", "kafka2-1:9092,kafka2-2:9092");
        final String topic1 = env("TOPIC1", "events");
        final String topic2 = env("TOPIC2", "events-routed");

        final String sourceBootstrap = "sink".equals(shape) ? k2 : k1;
        final String sourceTopic = "sink".equals(shape) ? topic2 : topic1;

        // Read once here, not inside the lambda: the lambda runs on the TaskManager,
        // where this process's environment does not exist.
        final String dropRoute = env("FILTER_DROP_ROUTE", "__none__");

        DataStream<Event> stream = env
                .fromSource(buildSource(sourceBootstrap, sourceTopic),
                        WatermarkStrategy.noWatermarks(), "kafka-source")
                .name("kafka-source")
                // The "filter" the operator's jobs do. Keeps everything by default so the
                // expected set is the full ledger; FILTER_DROP_ROUTE can drop one route.
                .filter(e -> !e.route.equals(dropRoute))
                .name("filter");

        if ("router".equals(shape)) {
            DeliveryGuarantee guarantee = DeliveryGuarantee.valueOf(
                    env("KAFKA_SINK_GUARANTEE", "NONE").toUpperCase().replace('-', '_'));
            KafkaSink<Event> kafkaSink = KafkaSink.<Event>builder()
                    .setBootstrapServers(k2)
                    .setRecordSerializer(KafkaRecordSerializationSchema.<Event>builder()
                            .setTopic(topic2)
                            .setKeySerializationSchema(
                                    (Event e) -> String.valueOf(e.eventId).getBytes(
                                            java.nio.charset.StandardCharsets.UTF_8))
                            .setValueSerializationSchema(
                                    (Event e) -> e.toWire().getBytes(
                                            java.nio.charset.StandardCharsets.UTF_8))
                            .build())
                    .setDeliveryGuarantee(guarantee)
                    .setTransactionalIdPrefix(env("TXN_ID_PREFIX", "loss-lab-router"))
                    .build();
            stream.sinkTo(kafkaSink).name("kafka-sink-" + guarantee);
            LOG.info("ROUTER shape: {} -> {} with guarantee {}", topic1, topic2, guarantee);
        } else {
            long now = System.currentTimeMillis();
            String window = env("SINK_FAIL_WINDOW", "off");
            long from = Long.MAX_VALUE, to = Long.MIN_VALUE;
            if (!"off".equals(window)) {
                String[] p = window.split("-", 2);
                from = Long.parseLong(p[0]);
                to = Long.parseLong(p[1]);
            }
            PgSink.FailureMode mode = PgSink.FailureMode.valueOf(
                    env("SINK_FAILURE_MODE", "none").toUpperCase().replace('-', '_'));
            PgSink sink = new PgSink(
                    env("PG_URL", "jdbc:postgresql://postgres:5432/lab"),
                    env("PG_USER", "lab"),
                    env("PG_PASSWORD", "lab"),
                    "on-conflict-ignore".equals(env("SINK_MODE", "plain-insert")),
                    Integer.parseInt(env("SINK_BATCH_SIZE", "100")),
                    mode, from, to,
                    Integer.parseInt(env("SINK_QUEUE_CAPACITY", "50")),
                    envBool("WRITE_PROGRESS", false),
                    env("JOB_NAME", "loss-lab"),
                    env("SINK_FAIL_TABLE", "t_b"),
                    Integer.parseInt(env("SINK_POISON_EVERY_N", "10")));
            stream.addSink(sink).name("pg-sink-" + mode);
            LOG.info("{} shape -> Postgres, sinkMode={} failureMode={} window={} (now={})",
                    shape, env("SINK_MODE", "plain-insert"), mode, window, now);
        }

        env.execute("loss-lab-" + shape + "-" + env("SCENARIO", "adhoc"));
    }
}
