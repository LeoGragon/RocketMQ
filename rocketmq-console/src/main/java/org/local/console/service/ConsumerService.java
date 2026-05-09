package org.local.console.service;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ConsumerService {

    private static final Logger log = LoggerFactory.getLogger(ConsumerService.class);

    private final Map<String, ConsumerSession> sessions = new ConcurrentHashMap<>();

    @Value("${rocketmq.namesrv-addr:127.0.0.1:9876}")
    private String namesrvAddr;

    public Map<String, Object> start(String topic, String consumerGroup, String tagFilter) {
        String sessionKey = topic + "|" + consumerGroup;

        if (sessions.containsKey(sessionKey)) {
            return Map.of("success", false, "error", "Consumer already running for this topic and group");
        }

        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer(consumerGroup);
        consumer.setNamesrvAddr(namesrvAddr);
        consumer.setInstanceName("console-consumer-" + System.currentTimeMillis());
        consumer.setAutoCommit(true);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setPullBatchSize(32);

        try {
            String subExpression = (tagFilter != null && !tagFilter.isEmpty()) ? tagFilter : "*";
            consumer.subscribe(topic, subExpression);
            consumer.start();

            ConsumerSession session = new ConsumerSession(consumer, topic, consumerGroup, tagFilter);
            sessions.put(sessionKey, session);

            log.info("Consumer started: topic={}, group={}, tag={}", topic, consumerGroup, subExpression);
            return Map.of("success", true, "sessionKey", sessionKey);
        } catch (Exception e) {
            log.error("Failed to start consumer: topic={}, group={}", topic, consumerGroup, e);
            try { consumer.shutdown(); } catch (Exception ignored) {}
            return Map.of("success", false, "error", e.getMessage());
        }
    }

    public Map<String, Object> poll(String sessionKey) {
        ConsumerSession session = sessions.get(sessionKey);
        if (session == null) {
            return Map.of("success", false, "error", "No active consumer session");
        }

        try {
            List<MessageExt> msgs = session.consumer.poll(3000);
            List<Map<String, Object>> messages = new ArrayList<>();

            if (msgs != null) {
                for (MessageExt msg : msgs) {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("msgId", msg.getMsgId());
                    m.put("topic", msg.getTopic());
                    m.put("tags", msg.getTags());
                    m.put("keys", msg.getKeys());
                    m.put("body", new String(msg.getBody(), StandardCharsets.UTF_8));
                    m.put("queueId", msg.getQueueId());
                    m.put("queueOffset", msg.getQueueOffset());
                    m.put("bornTimestamp", msg.getBornTimestamp());
                    m.put("storeTimestamp", msg.getStoreTimestamp());
                    m.put("reconsumeTimes", msg.getReconsumeTimes());
                    messages.add(m);
                }
            }

            session.totalConsumed += messages.size();
            return Map.of("success", true, "messages", messages, "totalConsumed", session.totalConsumed);
        } catch (Exception e) {
            log.error("Failed to poll messages: sessionKey={}", sessionKey, e);
            return Map.of("success", false, "error", e.getMessage());
        }
    }

    public Map<String, Object> stop(String sessionKey) {
        ConsumerSession session = sessions.remove(sessionKey);
        if (session == null) {
            return Map.of("success", false, "error", "No active consumer session");
        }

        try {
            session.consumer.shutdown();
            log.info("Consumer stopped: topic={}, group={}, totalConsumed={}",
                    session.topic, session.consumerGroup, session.totalConsumed);
            return Map.of("success", true, "totalConsumed", session.totalConsumed);
        } catch (Exception e) {
            log.error("Failed to stop consumer: sessionKey={}", sessionKey, e);
            return Map.of("success", false, "error", e.getMessage());
        }
    }

    public Map<String, Object> status(String sessionKey) {
        ConsumerSession session = sessions.get(sessionKey);
        if (session == null) {
            return Map.of("success", false, "error", "No active consumer session");
        }
        return Map.of(
                "success", true,
                "topic", session.topic,
                "consumerGroup", session.consumerGroup,
                "tagFilter", session.tagFilter,
                "totalConsumed", session.totalConsumed
        );
    }

    public List<Map<String, Object>> listSessions() {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Map.Entry<String, ConsumerSession> entry : sessions.entrySet()) {
            ConsumerSession s = entry.getValue();
            result.add(Map.of(
                    "sessionKey", entry.getKey(),
                    "topic", s.topic,
                    "consumerGroup", s.consumerGroup,
                    "tagFilter", s.tagFilter,
                    "totalConsumed", s.totalConsumed
            ));
        }
        return result;
    }

    private static class ConsumerSession {
        final DefaultLitePullConsumer consumer;
        final String topic;
        final String consumerGroup;
        final String tagFilter;
        int totalConsumed;

        ConsumerSession(DefaultLitePullConsumer consumer, String topic, String consumerGroup, String tagFilter) {
            this.consumer = consumer;
            this.topic = topic;
            this.consumerGroup = consumerGroup;
            this.tagFilter = tagFilter;
        }
    }
}
