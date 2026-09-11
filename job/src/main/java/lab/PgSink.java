package lab;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Properties;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Postgres sink with deliberately injectable failure behaviour.
 *
 * This class is the heart of the lab. The investigation in the vault left two surviving
 * root-cause hypotheses for the production incident, and one of them is "the source read
 * the backlog and the sink discarded it". {@link FailureMode} turns that hypothesis into
 * a controlled experiment instead of an argument.
 *
 * Failure modes:
 *   NONE              - insert, and let any exception fail the task (correct behaviour)
 *   SWALLOW           - catch the exception, log it, drop the batch, carry on.
 *                       This is the bug: offsets advance, rows never land, nothing red.
 *   FAIL_TASK         - same as NONE but forces an induced error inside the window, to
 *                       show that failing the task is recoverable where swallowing is not
 *   DROP_ON_FULL      - bounded in-memory queue that drops instead of back-pressuring
 *   ACK_BEFORE_COMMIT - "acknowledge" the batch before the transaction commits, then
 *                       lose the commit. The Postgres analogue of an async insert with
 *                       no wait-for-ack.
 */
public class PgSink extends RichSinkFunction<Event> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PgSink.class);

    public enum FailureMode { NONE, SWALLOW, FAIL_TASK, DROP_ON_FULL, ACK_BEFORE_COMMIT }

    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final boolean onConflictIgnore;
    private final int batchSize;
    private final FailureMode failureMode;
    private final long failFromMs;
    private final long failToMs;
    private final int queueCapacity;
    private final boolean writeProgress;
    private final String jobName;
    private final long flushIntervalMs = 1000L;

    private transient Connection conn;
    private transient List<Event> buffer;
    private transient Deque<Event> boundedQueue;
    private transient long droppedCount;
    private transient long lastFlushMs;
    private transient long swallowedCount;

    public PgSink(String jdbcUrl, String user, String password,
                  boolean onConflictIgnore, int batchSize,
                  FailureMode failureMode, long failFromMs, long failToMs,
                  int queueCapacity, boolean writeProgress, String jobName) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
        this.onConflictIgnore = onConflictIgnore;
        this.batchSize = batchSize;
        this.failureMode = failureMode;
        this.failFromMs = failFromMs;
        this.failToMs = failToMs;
        this.queueCapacity = queueCapacity;
        this.writeProgress = writeProgress;
        this.jobName = jobName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // NOT DriverManager.getConnection(). DriverManager lives on the system
        // classloader; the Postgres driver arrives on Flink's child-first USER
        // classloader. The first job submission happens to work, and every RESTART
        // then fails with "No suitable driver found" - which looks exactly like a
        // restart-only data bug and is not one. Instantiating the driver directly
        // sidesteps DriverManager's registry entirely.
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        conn = new org.postgresql.Driver().connect(jdbcUrl, props);
        if (conn == null) {
            throw new IllegalStateException("postgres driver refused the URL: " + jdbcUrl);
        }
        conn.setAutoCommit(false);
        buffer = new ArrayList<>(batchSize);
        boundedQueue = new ArrayDeque<>(queueCapacity);
        droppedCount = 0;
        swallowedCount = 0;
        lastFlushMs = System.currentTimeMillis();
        LOG.info("PgSink open: mode={} onConflictIgnore={} batchSize={} window={}..{}",
                failureMode, onConflictIgnore, batchSize, failFromMs, failToMs);
    }

    private boolean inFailureWindow() {
        if (failureMode == FailureMode.NONE) return false;
        long now = System.currentTimeMillis();
        return now >= failFromMs && now <= failToMs;
    }

    @Override
    public void invoke(Event e, Context ctx) throws Exception {
        if (failureMode == FailureMode.DROP_ON_FULL && inFailureWindow()) {
            if (boundedQueue.size() >= queueCapacity) {
                droppedCount++;
                if (droppedCount % 100 == 1) {
                    LOG.warn("DROP_ON_FULL: queue full, dropped {} events so far", droppedCount);
                }
                return; // the loss: dropped instead of back-pressuring
            }
            boundedQueue.add(e);
            if (boundedQueue.size() < queueCapacity) return;
            while (!boundedQueue.isEmpty()) buffer.add(boundedQueue.poll());
        } else {
            buffer.add(e);
        }
        // Size OR time, so a low-rate run still lands rows promptly.
        if (buffer.size() >= batchSize
                || System.currentTimeMillis() - lastFlushMs >= flushIntervalMs) {
            flush();
        }
    }

    private void flush() throws Exception {
        lastFlushMs = System.currentTimeMillis();
        if (buffer.isEmpty()) return;
        List<Event> batch = new ArrayList<>(buffer);
        buffer.clear();
        try {
            if (inFailureWindow() && failureMode == FailureMode.ACK_BEFORE_COMMIT) {
                writeBatch(batch);
                // "Acknowledged" - and then the commit never happens.
                conn.rollback();
                LOG.warn("ACK_BEFORE_COMMIT: {} rows acknowledged then rolled back", batch.size());
                return;
            }
            if (inFailureWindow() && failureMode == FailureMode.FAIL_TASK) {
                throw new RuntimeException("induced sink failure (FAIL_TASK) for "
                        + batch.size() + " rows");
            }
            if (inFailureWindow() && failureMode == FailureMode.SWALLOW) {
                throw new RuntimeException("induced sink failure (SWALLOW) for "
                        + batch.size() + " rows");
            }
            writeBatch(batch);
            conn.commit();
        } catch (Exception ex) {
            safeRollback();
            if (failureMode == FailureMode.SWALLOW && inFailureWindow()) {
                swallowedCount += batch.size();
                LOG.error("SWALLOW: dropped {} rows ({} total). The job stays green and "
                        + "the offsets keep advancing - this is the bug.",
                        batch.size(), swallowedCount, ex);
                return; // the loss
            }
            throw ex; // correct behaviour: fail the task, let Flink replay
        }
    }

    private void writeBatch(List<Event> batch) throws Exception {
        for (Event e : batch) {
            String table = e.targetTable();
            if (table == null) continue;
            String sql = "INSERT INTO " + table
                    + " (event_id, produced_at, route, src_topic, src_part, src_offset)"
                    + " VALUES (?,?,?,?,?,?)"
                    + (onConflictIgnore ? " ON CONFLICT (event_id) DO NOTHING" : "");
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setLong(1, e.eventId);
                ps.setLong(2, e.producedAt);
                ps.setString(3, e.route);
                ps.setString(4, e.srcTopic);
                ps.setInt(5, e.srcPartition);
                ps.setLong(6, e.srcOffset);
                ps.executeUpdate();
            }
        }
        if (writeProgress) {
            // The completion frontier, written ONLY after the data insert above.
            // Order matters: progress behind data replays (safe); progress ahead of
            // data loses (the bug we are eliminating).
            String sql = "INSERT INTO ingest_progress"
                    + " (job, topic, sink_table, partition, max_offset, updated_at)"
                    + " VALUES (?,?,?,?,?, now())"
                    + " ON CONFLICT (job, topic, sink_table, partition) DO UPDATE"
                    + " SET max_offset = GREATEST(ingest_progress.max_offset, EXCLUDED.max_offset),"
                    + "     updated_at = now()";
            for (Event e : batch) {
                String table = e.targetTable();
                if (table == null) continue;
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setString(1, jobName);
                    ps.setString(2, e.srcTopic);
                    ps.setString(3, table);
                    ps.setInt(4, e.srcPartition);
                    ps.setLong(5, e.srcOffset);
                    ps.executeUpdate();
                }
            }
        }
    }

    private void safeRollback() {
        try {
            if (conn != null && !conn.isClosed()) conn.rollback();
        } catch (Exception ignored) {
            // nothing useful to do here
        }
    }

    @Override
    public void finish() throws Exception {
        flush();
    }

    @Override
    public void close() throws Exception {
        try {
            flush();
        } finally {
            if (conn != null && !conn.isClosed()) conn.close();
        }
        LOG.info("PgSink closed: swallowed={} dropped={}", swallowedCount, droppedCount);
    }
}
