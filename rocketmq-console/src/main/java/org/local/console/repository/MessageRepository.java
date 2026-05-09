package org.local.console.repository;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public class MessageRepository {

    private static final Logger log = LoggerFactory.getLogger(MessageRepository.class);

    private final JdbcTemplate jdbc;

    public MessageRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @PostConstruct
    public void migrateSchema() {
        ensureColumn("sim_producer", "total_max_count",
                "ALTER TABLE sim_producer ADD COLUMN total_max_count BIGINT NOT NULL DEFAULT 0 AFTER interval_ms");
        ensureColumn("sim_producer", "last_run_sent_count",
                "ALTER TABLE sim_producer ADD COLUMN last_run_sent_count BIGINT NOT NULL DEFAULT 0 AFTER sent_count");
    }

    private void ensureColumn(String tableName, String columnName, String alterSql) {
        try {
            Long count = jdbc.queryForObject(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS " +
                            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?",
                    Long.class, tableName, columnName);
            if (count != null && count == 0) {
                jdbc.update(alterSql);
                log.info("Added missing column {}.{}", tableName, columnName);
            }
        } catch (Exception e) {
            log.warn("Failed to ensure column {}.{}: {}", tableName, columnName, e.getMessage());
        }
    }

    public void saveSent(String taskId, String topic, String tags, String keys,
                          String body, String msgId, int queueId, long queueOffset,
                          String messageType) {
        jdbc.update(
            "INSERT INTO sim_message (task_id, direction, topic, tags, msg_keys, body, msg_id, queue_id, queue_offset, message_type, born_timestamp, store_timestamp) " +
            "VALUES (?, 'SEND', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            taskId, topic, tags, keys, body, msgId, queueId, queueOffset, messageType,
            System.currentTimeMillis(), System.currentTimeMillis()
        );
    }

    public void saveConsumed(String taskId, String topic, String tags, String keys,
                              String body, String msgId, int queueId, long queueOffset,
                              long bornTimestamp, long storeTimestamp) {
        jdbc.update(
            "INSERT INTO sim_message (task_id, direction, topic, tags, msg_keys, body, msg_id, queue_id, queue_offset, message_type, born_timestamp, store_timestamp) " +
            "VALUES (?, 'CONSUME', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            taskId, topic, tags, keys, body, msgId, queueId, queueOffset, null,
            bornTimestamp, storeTimestamp
        );
    }

    public long countByTaskId(String taskId) {
        Long n = jdbc.queryForObject("SELECT COUNT(*) FROM sim_message WHERE task_id = ?", Long.class, taskId);
        return n != null ? n : 0;
    }

    public List<Map<String, Object>> findByTaskId(String taskId, int limit) {
        return jdbc.queryForList(
            "SELECT * FROM sim_message WHERE task_id = ? ORDER BY id DESC LIMIT ?", taskId, limit
        );
    }

    public List<Map<String, Object>> findAll(int offset, int limit) {
        return jdbc.queryForList(
            "SELECT * FROM sim_message ORDER BY id DESC LIMIT ? OFFSET ?", limit, offset
        );
    }

    public long count() {
        Long n = jdbc.queryForObject("SELECT COUNT(*) FROM sim_message", Long.class);
        return n != null ? n : 0;
    }

    public int deleteByTaskId(String taskId) {
        return jdbc.update("DELETE FROM sim_message WHERE task_id = ?", taskId);
    }

    // ── Producer persistence ─────────────────────────────────

    public void saveProducer(String taskId, String topic, String tags, String keys,
                              String messageType, Integer delayLevel, Boolean transactionCommit,
                              int intervalMs, long totalMaxCount, long maxCount, String bodyTemplate) {
        jdbc.update(
            "INSERT INTO sim_producer (task_id, topic, tags, msg_keys, message_type, delay_level, transaction_commit, interval_ms, total_max_count, max_count, body_template, status, start_time) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'RUNNING', NOW())",
            taskId, topic, tags, keys, messageType, delayLevel, transactionCommit,
            intervalMs, totalMaxCount, maxCount, bodyTemplate
        );
    }

    public void updateProducerStats(String taskId, long sentCount, long lastRunSentCount, long failedCount,
                                     String lastMsgId, String lastError) {
        jdbc.update(
            "UPDATE sim_producer SET sent_count = ?, last_run_sent_count = ?, failed_count = ?, last_msg_id = ?, last_error = ? WHERE task_id = ?",
            sentCount, lastRunSentCount, failedCount, lastMsgId, lastError, taskId
        );
    }

    public void updateProducerStopped(String taskId, long sentCount, long lastRunSentCount, long failedCount) {
        jdbc.update(
            "UPDATE sim_producer SET status = 'STOPPED', stop_time = NOW(), sent_count = ?, last_run_sent_count = ?, failed_count = ? WHERE task_id = ?",
            sentCount, lastRunSentCount, failedCount, taskId
        );
    }

    public void updateProducerRunning(String taskId) {
        jdbc.update(
            "UPDATE sim_producer SET status = 'RUNNING', start_time = NOW(), stop_time = NULL, last_run_sent_count = 0, last_error = NULL WHERE task_id = ?",
            taskId
        );
    }

    public List<Map<String, Object>> findAllProducers() {
        return jdbc.queryForList("SELECT * FROM sim_producer ORDER BY created_at DESC");
    }

    public Map<String, Object> findProducerById(String taskId) {
        List<Map<String, Object>> list = jdbc.queryForList(
            "SELECT * FROM sim_producer WHERE task_id = ?", taskId
        );
        return list.isEmpty() ? null : list.get(0);
    }

    public List<Map<String, Object>> findMessagesByTaskId(String taskId, int offset, int limit) {
        return jdbc.queryForList(
            "SELECT * FROM sim_message WHERE task_id = ? ORDER BY id DESC LIMIT ? OFFSET ?",
            taskId, limit, offset
        );
    }

    public long countMessagesByTaskId(String taskId) {
        Long n = jdbc.queryForObject(
            "SELECT COUNT(*) FROM sim_message WHERE task_id = ?", Long.class, taskId
        );
        return n != null ? n : 0;
    }

    public void deleteProducer(String taskId) {
        jdbc.update("DELETE FROM sim_producer WHERE task_id = ?", taskId);
    }

    // ── Consumer persistence ─────────────────────────────────

    public void saveConsumer(String taskId, String topic, String consumerGroup,
                             String tagFilter, int pollIntervalMs) {
        jdbc.update(
            "INSERT INTO sim_consumer (task_id, topic, consumer_group, tag_filter, poll_interval_ms, status, start_time) " +
            "VALUES (?, ?, ?, ?, ?, 'RUNNING', NOW())",
            taskId, topic, consumerGroup, tagFilter, pollIntervalMs
        );
    }

    public void updateConsumerStats(String taskId, long consumedCount, String lastError) {
        jdbc.update(
            "UPDATE sim_consumer SET consumed_count = ?, last_error = ? WHERE task_id = ?",
            consumedCount, lastError, taskId
        );
    }

    public void updateConsumerStopped(String taskId, long consumedCount) {
        jdbc.update(
            "UPDATE sim_consumer SET status = 'STOPPED', stop_time = NOW(), consumed_count = ? WHERE task_id = ?",
            consumedCount, taskId
        );
    }

    public void updateConsumerRunning(String taskId) {
        jdbc.update(
            "UPDATE sim_consumer SET status = 'RUNNING', start_time = NOW(), stop_time = NULL, last_error = NULL WHERE task_id = ?",
            taskId
        );
    }

    public List<Map<String, Object>> findAllConsumers() {
        return jdbc.queryForList("SELECT * FROM sim_consumer ORDER BY created_at DESC");
    }

    public Map<String, Object> findConsumerById(String taskId) {
        List<Map<String, Object>> list = jdbc.queryForList(
            "SELECT * FROM sim_consumer WHERE task_id = ?", taskId
        );
        return list.isEmpty() ? null : list.get(0);
    }

    public List<Map<String, Object>> findConsumedMessagesByTaskId(String taskId, int offset, int limit) {
        return jdbc.queryForList(
            "SELECT * FROM sim_message WHERE task_id = ? AND direction = 'CONSUME' ORDER BY id DESC LIMIT ? OFFSET ?",
            taskId, limit, offset
        );
    }

    public long countConsumedMessagesByTaskId(String taskId) {
        Long n = jdbc.queryForObject(
            "SELECT COUNT(*) FROM sim_message WHERE task_id = ? AND direction = 'CONSUME'",
            Long.class, taskId
        );
        return n != null ? n : 0;
    }

    public void deleteConsumer(String taskId) {
        jdbc.update("DELETE FROM sim_consumer WHERE task_id = ?", taskId);
    }
}
