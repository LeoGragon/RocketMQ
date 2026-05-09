package org.local.console.service;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.local.console.repository.MessageRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class SimulationService {

    private static final Logger log = LoggerFactory.getLogger(SimulationService.class);

    private final Map<String, SimulationTask> tasks = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(
            Runtime.getRuntime().availableProcessors() * 2);
    private final MessageRepository messageRepository;

    @Value("${rocketmq.namesrv-addr:127.0.0.1:9876}")
    private String namesrvAddr;

    public SimulationService(MessageRepository messageRepository) {
        this.messageRepository = messageRepository;
    }

    @PostConstruct
    public void init() {
        // Reload persisted tasks on startup. Tasks that were RUNNING are
        // marked STOPPED because the JVM restart killed the actual clients.
        List<Map<String, Object>> producers = messageRepository.findAllProducers();
        for (Map<String, Object> p : producers) {
            String taskId = (String) p.get("task_id");
            String status = (String) p.get("status");
            if ("RUNNING".equals(status)) {
                messageRepository.updateProducerStopped(taskId,
                        ((Number) p.get("sent_count")).longValue(),
                        toLong(p.get("last_run_sent_count")),
                        ((Number) p.get("failed_count")).longValue());
            }
            tasks.put(taskId, new ProducerHistoryTask(p));
        }

        List<Map<String, Object>> consumers = messageRepository.findAllConsumers();
        for (Map<String, Object> c : consumers) {
            String taskId = (String) c.get("task_id");
            String status = (String) c.get("status");
            if ("RUNNING".equals(status)) {
                messageRepository.updateConsumerStopped(taskId,
                        ((Number) c.get("consumed_count")).longValue());
            }
            tasks.put(taskId, new ConsumerHistoryTask(c));
        }

        if (!tasks.isEmpty()) {
            log.info("Loaded {} task history records from database", tasks.size());
        }
    }

    // ── Producer ────────────────────────────────────────────────

    public Map<String, Object> startProducer(String topic, String tags, String keys,
                                              String messageType, Integer delayLevel,
                                              Boolean transactionCommit,
                                              int intervalMs, long totalMaxCount, long maxCount,
                                              String bodyTemplate) {
        String taskId = UUID.randomUUID().toString().substring(0, 8);
        return startProducerTask(taskId, topic, tags, keys, messageType, delayLevel,
                transactionCommit, intervalMs, totalMaxCount, maxCount, bodyTemplate,
                0, 0, null, null, true);
    }

    private Map<String, Object> startProducerTask(String taskId, String topic, String tags, String keys,
                                                  String messageType, Integer delayLevel,
                                                  Boolean transactionCommit,
                                                  int intervalMs, long totalMaxCount, long maxCount,
                                                  String bodyTemplate,
                                                  long initialSentCount,
                                                  long initialFailedCount,
                                                  String lastMsgId,
                                                  String lastError,
                                                  boolean persistNewRecord) {
        try {
            String group = "sim-producer-" + taskId;
            DefaultMQProducer producer;

            if ("TRANSACTION".equals(messageType)) {
                TransactionMQProducer tx = new TransactionMQProducer(group);
                tx.setNamesrvAddr(namesrvAddr);
                tx.setInstanceName("sim-tx-" + taskId);
                tx.setSendMsgTimeout(10000);
                tx.setRetryTimesWhenSendFailed(0);
                tx.setVipChannelEnabled(false);
                ExecutorService txExecutor = Executors.newSingleThreadExecutor();
                tx.setExecutorService(txExecutor);
                tx.setTransactionListener(new TransactionListener() {
                    @Override
                    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                        if (arg instanceof LocalTransactionState) return (LocalTransactionState) arg;
                        return LocalTransactionState.COMMIT_MESSAGE;
                    }

                    @Override
                    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                        return LocalTransactionState.COMMIT_MESSAGE;
                    }
                });
                tx.start();
                producer = tx;
            } else {
                producer = new DefaultMQProducer(group);
                producer.setNamesrvAddr(namesrvAddr);
                producer.setInstanceName("sim-producer-" + taskId);
                producer.setSendMsgTimeout(10000);
                producer.setRetryTimesWhenSendFailed(0);
                producer.setVipChannelEnabled(false);
                producer.setSendLatencyFaultEnable(false);
                producer.start();
            }

            ProducerTask task = new ProducerTask(taskId, topic, tags, keys, messageType,
                    delayLevel, transactionCommit, intervalMs, totalMaxCount, maxCount,
                    bodyTemplate, producer);
            task.sentCount.set(initialSentCount);
            task.failedCount.set(initialFailedCount);
            task.runStartSentCount = initialSentCount;
            task.lastMsgId = lastMsgId;
            task.lastError = lastError;
            tasks.put(taskId, task);

            try {
                if (persistNewRecord) {
                    messageRepository.saveProducer(taskId, topic, tags, keys, messageType,
                            delayLevel, transactionCommit, intervalMs, totalMaxCount, maxCount, bodyTemplate);
                } else {
                    messageRepository.updateProducerRunning(taskId);
                }
            } catch (Exception e) {
                log.warn("Failed to persist producer running state: {}", e.getMessage());
            }

            task.future = scheduler.scheduleWithFixedDelay(() -> sendOne(task),
                    0, intervalMs, TimeUnit.MILLISECONDS);

            log.info("Producer started: taskId={}, topic={}, type={}, interval={}ms",
                    taskId, topic, messageType, intervalMs);

            return Map.of("success", true, "taskId", taskId);
        } catch (Exception e) {
            log.error("Failed to start producer: topic={}", topic, e);
            return Map.of("success", false, "error", e.getMessage());
        }
    }

    public Map<String, Object> restartProducer(String taskId) {
        SimulationTask existing = tasks.get(taskId);
        if (existing != null && existing.running) {
            return Map.of("success", false, "error", "Task is already running: " + taskId);
        }

        Map<String, Object> row = messageRepository.findProducerById(taskId);
        if (row == null) {
            return Map.of("success", false, "error", "Producer not found: " + taskId);
        }

        return startProducerTask(
                taskId,
                toStringValue(row.get("topic")),
                toStringValue(row.get("tags")),
                toStringValue(row.get("msg_keys")),
                toStringValue(row.get("message_type"), "NORMAL"),
                toInteger(row.get("delay_level")),
                toBoolean(row.get("transaction_commit")),
                toInt(row.get("interval_ms"), 1000),
                toLong(row.get("total_max_count")),
                toLong(row.get("max_count")),
                toStringValue(row.get("body_template")),
                toLong(row.get("sent_count")),
                toLong(row.get("failed_count")),
                toStringValue(row.get("last_msg_id")),
                null,
                false
        );
    }

    private void sendOne(ProducerTask task) {
        if (!task.running) return;
        if (task.totalMaxCount > 0 && task.sentCount.get() >= task.totalMaxCount) {
            stopTask(task.taskId);
            return;
        }
        if (task.maxCount > 0 && task.sentCount.get() - task.runStartSentCount >= task.maxCount) {
            stopTask(task.taskId);
            return;
        }

        try {
            String body = task.bodyTemplate != null && !task.bodyTemplate.isEmpty()
                    ? task.bodyTemplate.replace("{{seq}}", String.valueOf(task.sentCount.get() + 1))
                    .replace("{{time}}", String.valueOf(System.currentTimeMillis()))
                    : "sim-msg-" + (task.sentCount.get() + 1) + "-" + System.currentTimeMillis();

            Message msg = new Message(task.topic,
                    task.tags != null && !task.tags.isEmpty() ? task.tags : "*",
                    task.keys,
                    body.getBytes(StandardCharsets.UTF_8));

            SendResult result;
            switch (task.messageType) {
                case "ORDER":
                    result = task.producer.send(msg, (MessageQueueSelector) (mqs, m, arg) -> {
                        int hash = arg != null ? Math.abs(arg.hashCode()) : 0;
                        return mqs.get(hash % mqs.size());
                    }, task.keys != null ? task.keys : "default");
                    break;
                case "DELAY":
                    int dl = task.delayLevel != null ? task.delayLevel : 3;
                    msg.setDelayTimeLevel(dl);
                    result = task.producer.send(msg);
                    break;
                case "TRANSACTION":
                    TransactionMQProducer tx = (TransactionMQProducer) task.producer;
                    LocalTransactionState state = Boolean.TRUE.equals(task.transactionCommit)
                            ? LocalTransactionState.COMMIT_MESSAGE
                            : LocalTransactionState.ROLLBACK_MESSAGE;
                    result = tx.sendMessageInTransaction(msg, state);
                    break;
                default:
                    result = task.producer.send(msg);
                    break;
            }

            task.sentCount.incrementAndGet();
            task.lastSendTime = System.currentTimeMillis();
            task.lastMsgId = result.getMsgId();

            // Persist to DB
            try {
                messageRepository.saveSent(task.taskId, task.topic,
                        task.tags, task.keys, body,
                        result.getMsgId(),
                        result.getMessageQueue().getQueueId(),
                        result.getQueueOffset(),
                        task.messageType);
            } catch (Exception e) {
                log.warn("Failed to persist sent message: {}", e.getMessage());
            }

            // Periodic DB stats update (every 10 messages)
            if (task.sentCount.get() % 10 == 0) {
                try {
                    messageRepository.updateProducerStats(task.taskId,
                            task.sentCount.get(), task.getRunSentCount(), task.failedCount.get(),
                            task.lastMsgId, task.lastError);
                } catch (Exception e) {
                    log.warn("Failed to update producer stats: {}", e.getMessage());
                }
            }

        } catch (Exception e) {
            task.failedCount.incrementAndGet();
            task.lastError = e.getMessage();
        }
    }

    // ── Consumer ────────────────────────────────────────────────

    public Map<String, Object> startConsumer(String topic, String consumerGroup,
                                              String tagFilter, int pollIntervalMs) {
        String taskId = UUID.randomUUID().toString().substring(0, 8);
        return startConsumerTask(taskId, topic, consumerGroup, tagFilter, pollIntervalMs,
                0, null, true);
    }

    private Map<String, Object> startConsumerTask(String taskId, String topic, String consumerGroup,
                                                  String tagFilter, int pollIntervalMs,
                                                  long initialConsumedCount,
                                                  String lastError,
                                                  boolean persistNewRecord) {
        try {
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
            consumer.setNamesrvAddr(namesrvAddr);
            consumer.setInstanceName("sim-consumer-" + taskId);
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

            String subExpression = (tagFilter != null && !tagFilter.isEmpty()) ? tagFilter : "*";
            consumer.subscribe(topic, subExpression);

            ConsumerTask task = new ConsumerTask(taskId, topic, consumerGroup,
                    tagFilter, pollIntervalMs, consumer);
            task.consumedCount.set(initialConsumedCount);
            task.runStartConsumedCount = initialConsumedCount;
            task.lastError = lastError;
            tasks.put(taskId, task);

            try {
                if (persistNewRecord) {
                    messageRepository.saveConsumer(taskId, topic, consumerGroup, tagFilter, pollIntervalMs);
                } else {
                    messageRepository.updateConsumerRunning(taskId);
                }
            } catch (Exception e) {
                log.warn("Failed to persist consumer running state: {}", e.getMessage());
            }

            consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
                if (!task.running) return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                task.consumedCount.addAndGet(msgs.size());
                task.lastPollTime = System.currentTimeMillis();
                for (MessageExt msg : msgs) {
                    try {
                        messageRepository.saveConsumed(task.taskId, msg.getTopic(),
                                msg.getTags(), msg.getKeys(),
                                new String(msg.getBody(), StandardCharsets.UTF_8),
                                msg.getMsgId(), msg.getQueueId(), msg.getQueueOffset(),
                                msg.getBornTimestamp(), msg.getStoreTimestamp());
                    } catch (Exception e) {
                        log.warn("Failed to persist consumed message: {}", e.getMessage());
                    }
                }
                // Periodic DB stats update (every 10 messages)
                if (task.consumedCount.get() % 10 == 0) {
                    try {
                        messageRepository.updateConsumerStats(task.taskId,
                                task.consumedCount.get(), task.lastError);
                    } catch (Exception e) {
                        log.warn("Failed to update consumer stats: {}", e.getMessage());
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            });

            consumer.start();

            log.info("Consumer started: taskId={}, topic={}, group={}", taskId, topic, consumerGroup);

            return Map.of("success", true, "taskId", taskId);
        } catch (Exception e) {
            log.error("Failed to start consumer: topic={}, group={}", topic, consumerGroup, e);
            return Map.of("success", false, "error", e.getMessage());
        }
    }

    public Map<String, Object> restartConsumer(String taskId) {
        SimulationTask existing = tasks.get(taskId);
        if (existing != null && existing.running) {
            return Map.of("success", false, "error", "Task is already running: " + taskId);
        }

        Map<String, Object> row = messageRepository.findConsumerById(taskId);
        if (row == null) {
            return Map.of("success", false, "error", "Consumer not found: " + taskId);
        }

        return startConsumerTask(
                taskId,
                toStringValue(row.get("topic")),
                toStringValue(row.get("consumer_group")),
                toStringValue(row.get("tag_filter")),
                toInt(row.get("poll_interval_ms"), 1000),
                toLong(row.get("consumed_count")),
                null,
                false
        );
    }

    // ── Control ─────────────────────────────────────────────────

    public Map<String, Object> stopTask(String taskId) {
        SimulationTask task = tasks.get(taskId);
        if (task == null) {
            return Map.of("success", false, "error", "Task not found: " + taskId);
        }

        task.running = false;
        task.stopTime = System.currentTimeMillis();
        if (task.future != null) {
            task.future.cancel(false);
        }

        try {
            task.shutdown();
        } catch (Exception e) {
            log.warn("Error shutting down task {}: {}", taskId, e.getMessage());
        }

        // Persist stopped state to DB
        if (task.getType().equals("PRODUCER")) {
            try {
                messageRepository.updateProducerStopped(taskId, task.getSentCount(),
                        task.getRunSentCount(), task.getFailedCount());
            } catch (Exception e) {
                log.warn("Failed to update producer stopped: {}", e.getMessage());
            }
        } else if (task.getType().equals("CONSUMER")) {
            try {
                messageRepository.updateConsumerStopped(taskId, task.getConsumedCount());
            } catch (Exception e) {
                log.warn("Failed to update consumer stopped: {}", e.getMessage());
            }
        }

        log.info("Task stopped: taskId={}, type={}, sent={}, consumed={}",
                taskId, task.getType(), task.getSentCount(), task.getConsumedCount());

        return Map.of("success", true, "taskId", taskId,
                "sentCount", task.getSentCount(),
                "consumedCount", task.getConsumedCount());
    }

    public Map<String, Object> stopAll() {
        List<String> ids = new ArrayList<>(tasks.keySet());
        int count = 0;
        for (String id : ids) {
            Map<String, Object> r = stopTask(id);
            if (Boolean.TRUE.equals(r.get("success"))) count++;
        }
        return Map.of("success", true, "stopped", count);
    }

    public Map<String, Object> removeTask(String taskId) {
        SimulationTask task = tasks.get(taskId);
        if (task == null) {
            return Map.of("success", false, "error", "Task not found: " + taskId);
        }
        if (task.running) {
            return Map.of("success", false, "error", "Stop the task before removing it");
        }
        tasks.remove(taskId);
        // Delete from DB
        try {
            if ("PRODUCER".equals(task.getType())) {
                messageRepository.deleteProducer(taskId);
            } else {
                messageRepository.deleteConsumer(taskId);
            }
            messageRepository.deleteByTaskId(taskId);
        } catch (Exception e) {
            log.warn("Failed to delete task from DB: {}", e.getMessage());
        }
        return Map.of("success", true, "taskId", taskId);
    }

    public List<Map<String, Object>> getTasks() {
        List<Map<String, Object>> result = new ArrayList<>();
        long now = System.currentTimeMillis();

        for (SimulationTask task : tasks.values()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("taskId", task.taskId);
            m.put("type", task.getType());
            m.put("topic", task.topic);
            m.put("messageType", task.getMessageType());
            m.put("sentCount", task.getSentCount());
            m.put("runSentCount", task.getRunSentCount());
            m.put("totalMaxCount", task.getTotalMaxCount());
            m.put("runMaxCount", task.getRunMaxCount());
            m.put("failedCount", task.getFailedCount());
            m.put("consumedCount", task.getConsumedCount());
            m.put("running", task.running);

            long endTime = task.running ? now : (task.stopTime > 0 ? task.stopTime : now);
            long elapsed = task.startTime > 0 ? endTime - task.startTime : 0;
            m.put("elapsedMs", elapsed);

            double tps = 0;
            long eventCount = "PRODUCER".equals(task.getType())
                    ? task.getRunSentCount()
                    : task.getRunConsumedCount();
            if (elapsed > 0 && eventCount > 0) {
                tps = eventCount * 1000.0 / elapsed;
            }
            m.put("tps", Math.round(tps * 10) / 10.0);

            m.put("lastMsgId", task.getLastMsgId());
            m.put("lastError", task.getLastError());

            result.add(m);
        }
        return result;
    }

    public List<Map<String, Object>> getMessages(String taskId, int limit) {
        return messageRepository.findByTaskId(taskId, limit);
    }

    public List<Map<String, Object>> getAllMessages(int offset, int limit) {
        return messageRepository.findAll(offset, limit);
    }

    public long getMessageCount(String taskId) {
        return messageRepository.countByTaskId(taskId);
    }

    public long getTotalMessageCount() {
        return messageRepository.count();
    }

    // ── Producer history ───────────────────────────────────────

    public List<Map<String, Object>> getProducerHistory() {
        return messageRepository.findAllProducers();
    }

    public Map<String, Object> getProducerDetail(String taskId) {
        Map<String, Object> producer = messageRepository.findProducerById(taskId);
        if (producer == null) return null;

        long msgCount = messageRepository.countMessagesByTaskId(taskId);
        producer.put("totalMessages", msgCount);
        return producer;
    }

    public List<Map<String, Object>> getProducerMessages(String taskId, int offset, int limit) {
        return messageRepository.findMessagesByTaskId(taskId, offset, limit);
    }

    public long getProducerMessageCount(String taskId) {
        return messageRepository.countMessagesByTaskId(taskId);
    }

    // ── Consumer history ───────────────────────────────────────

    public List<Map<String, Object>> getConsumerHistory() {
        return messageRepository.findAllConsumers();
    }

    public Map<String, Object> getConsumerDetail(String taskId) {
        Map<String, Object> consumer = messageRepository.findConsumerById(taskId);
        if (consumer == null) return null;

        long msgCount = messageRepository.countConsumedMessagesByTaskId(taskId);
        consumer.put("totalMessages", msgCount);
        return consumer;
    }

    public List<Map<String, Object>> getConsumerMessages(String taskId, int offset, int limit) {
        return messageRepository.findConsumedMessagesByTaskId(taskId, offset, limit);
    }

    public long getConsumerMessageCount(String taskId) {
        return messageRepository.countConsumedMessagesByTaskId(taskId);
    }

    // ── Inner classes ───────────────────────────────────────────

    private abstract static class SimulationTask {
        final String taskId;
        final String topic;
        volatile boolean running = true;
        volatile ScheduledFuture<?> future;
        volatile long startTime = System.currentTimeMillis();
        volatile long stopTime;

        SimulationTask(String taskId, String topic) {
            this.taskId = taskId;
            this.topic = topic;
        }

        abstract String getType();
        abstract long getSentCount();
        abstract long getFailedCount();
        abstract long getConsumedCount();
        abstract long getRunSentCount();
        abstract long getRunConsumedCount();
        abstract long getTotalMaxCount();
        abstract long getRunMaxCount();
        abstract String getMessageType();
        abstract String getLastMsgId();
        abstract String getLastError();
        abstract void shutdown() throws Exception;
    }

    private static class ProducerTask extends SimulationTask {
        final String tags, keys, messageType, bodyTemplate;
        final Integer delayLevel;
        final Boolean transactionCommit;
        final long totalMaxCount;
        final long maxCount;
        final DefaultMQProducer producer;
        final AtomicLong sentCount = new AtomicLong(0);
        final AtomicLong failedCount = new AtomicLong(0);
        volatile long runStartSentCount;
        volatile long lastSendTime;
        volatile String lastMsgId;
        volatile String lastError;

        ProducerTask(String taskId, String topic, String tags, String keys,
                     String messageType, Integer delayLevel, Boolean transactionCommit,
                     int intervalMs, long totalMaxCount, long maxCount, String bodyTemplate,
                     DefaultMQProducer producer) {
            super(taskId, topic);
            this.tags = tags;
            this.keys = keys;
            this.messageType = messageType;
            this.delayLevel = delayLevel;
            this.transactionCommit = transactionCommit;
            this.totalMaxCount = totalMaxCount;
            this.maxCount = maxCount;
            this.bodyTemplate = bodyTemplate;
            this.producer = producer;
        }

        @Override String getType() { return "PRODUCER"; }
        @Override long getSentCount() { return sentCount.get(); }
        @Override long getFailedCount() { return failedCount.get(); }
        @Override long getConsumedCount() { return 0; }
        @Override long getRunSentCount() { return Math.max(0, sentCount.get() - runStartSentCount); }
        @Override long getRunConsumedCount() { return 0; }
        @Override long getTotalMaxCount() { return totalMaxCount; }
        @Override long getRunMaxCount() { return maxCount; }
        @Override String getMessageType() { return messageType; }
        @Override String getLastMsgId() { return lastMsgId; }
        @Override String getLastError() { return lastError; }
        @Override void shutdown() throws Exception { producer.shutdown(); }
    }

    private static class ConsumerTask extends SimulationTask {
        final String consumerGroup, tagFilter;
        final DefaultMQPushConsumer consumer;
        final AtomicLong consumedCount = new AtomicLong(0);
        volatile long runStartConsumedCount;
        volatile long lastPollTime;
        volatile String lastError;

        ConsumerTask(String taskId, String topic, String consumerGroup,
                     String tagFilter, int pollIntervalMs,
                     DefaultMQPushConsumer consumer) {
            super(taskId, topic);
            this.consumerGroup = consumerGroup;
            this.tagFilter = tagFilter;
            this.consumer = consumer;
        }

        @Override String getType() { return "CONSUMER"; }
        @Override long getSentCount() { return 0; }
        @Override long getFailedCount() { return 0; }
        @Override long getConsumedCount() { return consumedCount.get(); }
        @Override long getRunSentCount() { return 0; }
        @Override long getRunConsumedCount() { return Math.max(0, consumedCount.get() - runStartConsumedCount); }
        @Override long getTotalMaxCount() { return 0; }
        @Override long getRunMaxCount() { return 0; }
        @Override String getMessageType() { return "-"; }
        @Override String getLastMsgId() { return null; }
        @Override String getLastError() { return lastError; }
        @Override void shutdown() throws Exception { consumer.shutdown(); }
    }

    // History placeholders loaded from DB on startup (always stopped)

    private static class ProducerHistoryTask extends SimulationTask {
        private final String tags, keys, messageType, lastMsgId, lastError;
        private final long sentCount, lastRunSentCount, failedCount, totalMaxCount, maxCount;
        private final long startTimeMs, stopTimeMs;

        ProducerHistoryTask(Map<String, Object> row) {
            super((String) row.get("task_id"), (String) row.get("topic"));
            this.tags = (String) row.get("tags");
            this.keys = (String) row.get("msg_keys");
            this.messageType = (String) row.get("message_type");
            this.lastMsgId = (String) row.get("last_msg_id");
            this.lastError = (String) row.get("last_error");
            this.sentCount = toLong(row.get("sent_count"));
            this.lastRunSentCount = toLong(row.get("last_run_sent_count"));
            this.failedCount = toLong(row.get("failed_count"));
            this.totalMaxCount = toLong(row.get("total_max_count"));
            this.maxCount = toLong(row.get("max_count"));
            this.startTimeMs = toTimestamp(row.get("start_time"));
            this.stopTimeMs = toTimestamp(row.get("stop_time"));
            this.running = false;
            this.startTime = startTimeMs > 0 ? startTimeMs : System.currentTimeMillis();
            this.stopTime = stopTimeMs > 0 ? stopTimeMs : System.currentTimeMillis();
        }

        @Override String getType() { return "PRODUCER"; }
        @Override long getSentCount() { return sentCount; }
        @Override long getFailedCount() { return failedCount; }
        @Override long getConsumedCount() { return 0; }
        @Override long getRunSentCount() { return lastRunSentCount; }
        @Override long getRunConsumedCount() { return 0; }
        @Override long getTotalMaxCount() { return totalMaxCount; }
        @Override long getRunMaxCount() { return maxCount; }
        @Override String getMessageType() { return messageType; }
        @Override String getLastMsgId() { return lastMsgId; }
        @Override String getLastError() { return lastError; }
        @Override void shutdown() {}
    }

    private static class ConsumerHistoryTask extends SimulationTask {
        private final String consumerGroup, tagFilter, lastError;
        private final long consumedCount;
        private final long startTimeMs, stopTimeMs;

        ConsumerHistoryTask(Map<String, Object> row) {
            super((String) row.get("task_id"), (String) row.get("topic"));
            this.consumerGroup = (String) row.get("consumer_group");
            this.tagFilter = (String) row.get("tag_filter");
            this.lastError = (String) row.get("last_error");
            this.consumedCount = toLong(row.get("consumed_count"));
            this.startTimeMs = toTimestamp(row.get("start_time"));
            this.stopTimeMs = toTimestamp(row.get("stop_time"));
            this.running = false;
            this.startTime = startTimeMs > 0 ? startTimeMs : System.currentTimeMillis();
            this.stopTime = stopTimeMs > 0 ? stopTimeMs : System.currentTimeMillis();
        }

        @Override String getType() { return "CONSUMER"; }
        @Override long getSentCount() { return 0; }
        @Override long getFailedCount() { return 0; }
        @Override long getConsumedCount() { return consumedCount; }
        @Override long getRunSentCount() { return 0; }
        @Override long getRunConsumedCount() { return consumedCount; }
        @Override long getTotalMaxCount() { return 0; }
        @Override long getRunMaxCount() { return 0; }
        @Override String getMessageType() { return "-"; }
        @Override String getLastMsgId() { return null; }
        @Override String getLastError() { return lastError; }
        @Override void shutdown() {}
    }

    private static long toLong(Object v) {
        return v instanceof Number ? ((Number) v).longValue() : 0L;
    }

    private static int toInt(Object v, int defaultValue) {
        return v instanceof Number ? ((Number) v).intValue() : defaultValue;
    }

    private static Integer toInteger(Object v) {
        return v instanceof Number ? ((Number) v).intValue() : null;
    }

    private static Boolean toBoolean(Object v) {
        if (v instanceof Boolean) return (Boolean) v;
        if (v instanceof Number) return ((Number) v).intValue() != 0;
        if (v instanceof String) return Boolean.parseBoolean((String) v);
        return null;
    }

    private static String toStringValue(Object v) {
        return toStringValue(v, null);
    }

    private static String toStringValue(Object v, String defaultValue) {
        return v != null ? String.valueOf(v) : defaultValue;
    }

    private static long toTimestamp(Object v) {
        if (v == null) return 0L;
        if (v instanceof java.sql.Timestamp) return ((java.sql.Timestamp) v).getTime();
        if (v instanceof java.util.Date) return ((java.util.Date) v).getTime();
        return 0L;
    }
}
