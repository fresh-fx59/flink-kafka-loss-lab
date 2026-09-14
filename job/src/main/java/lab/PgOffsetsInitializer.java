package lab;

import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Resume from a position stored in the SINK, not in Kafka and not in a checkpoint.
 *
 * Two modes, deliberately, because the difference between them is the whole point:
 *
 *   MAX_OFFSET  - resume one past MAX(src_offset) in a single destination table.
 *                 This is the obvious implementation and it is WRONG. A maximum is not
 *                 a completed prefix: if the batch for offsets 1-100 fails while a
 *                 later batch 101-200 succeeds, the maximum is 200 and everything in
 *                 1-100 is lost forever. With fan-out it is worse - one table can be
 *                 ahead of another and resuming at the leader's maximum drops the
 *                 laggard's tail.
 *
 *   FRONTIER    - resume one past MIN over every branch's recorded progress, where each
 *                 branch writes its progress row only AFTER its data insert is
 *                 acknowledged. Progress can then sit behind the data (replay, safe)
 *                 but never ahead of it (loss).
 */
public class PgOffsetsInitializer implements OffsetsInitializer {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PgOffsetsInitializer.class);

    public enum Mode { MAX_OFFSET, FRONTIER }

    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final Mode mode;
    private final String leaderTable;
    private final String jobName;

    public PgOffsetsInitializer(String jdbcUrl, String user, String password,
                                Mode mode, String leaderTable, String jobName) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
        this.mode = mode;
        this.leaderTable = leaderTable;
        this.jobName = jobName;
    }

    @Override
    public Map<TopicPartition, Long> getPartitionOffsets(
            Collection<TopicPartition> partitions,
            PartitionOffsetsRetriever retriever) {

        Map<TopicPartition, Long> earliest = retriever.beginningOffsets(partitions);
        Map<TopicPartition, Long> result = new HashMap<>();
        Map<Integer, Long> stored = new HashMap<>();

        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);

        String sql = (mode == Mode.MAX_OFFSET)
                // The naive version: the high-water mark of ONE table.
                ? "SELECT src_part, MAX(src_offset) FROM " + leaderTable + " GROUP BY src_part"
                // The safe version: the lowest frontier across every branch.
                : "SELECT partition, MIN(max_offset) FROM ingest_progress"
                  + " WHERE job = ? GROUP BY partition";

        try (Connection c = new org.postgresql.Driver().connect(jdbcUrl, props);
             PreparedStatement ps = c.prepareStatement(sql)) {
            if (mode == Mode.FRONTIER) {
                ps.setString(1, jobName);
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    stored.put(rs.getInt(1), rs.getLong(2));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("could not read the resume point from Postgres", e);
        }

        for (TopicPartition tp : partitions) {
            Long s = stored.get(tp.partition());
            // No stored position for a partition means nothing was ever written from
            // it, so start at the beginning. Falling back to LATEST here would be the
            // same silent-skip bug this class exists to avoid.
            long start = (s == null) ? earliest.getOrDefault(tp, 0L) : s + 1;
            result.put(tp, start);
            LOG.info("PgOffsetsInitializer[{}] {} -> starting offset {}", mode, tp, start);
        }
        return result;
    }

    @Override
    public OffsetResetStrategy getAutoOffsetResetStrategy() {
        return OffsetResetStrategy.EARLIEST;
    }
}
