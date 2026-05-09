package org.local.console.service;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Service
public class MessageService {

    private static final Logger log = LoggerFactory.getLogger(MessageService.class);

    private final DefaultMQProducer producer;
    private final TransactionMQProducer txProducer;

    public MessageService(@Qualifier("defaultMQProducer") DefaultMQProducer producer,
                          @Qualifier("transactionMQProducer") TransactionMQProducer txProducer) {
        this.producer = producer;
        this.txProducer = txProducer;
    }

    public Map<String, Object> sendMessage(String topic, String tags, String keys,
                                           String body, String messageType,
                                           Integer delayLevel, Boolean transactionCommit) {
        Map<String, Object> result = new HashMap<>();
        try {
            Message msg = new Message(topic,
                    tags != null && !tags.isEmpty() ? tags : "*",
                    keys,
                    body.getBytes(StandardCharsets.UTF_8));

            SendResult sendResult;
            switch (messageType) {
                case "ORDER":
                    sendResult = producer.send(msg, (MessageQueueSelector) (mqs, m, arg) -> {
                        int hash = arg != null ? Math.abs(arg.hashCode()) : 0;
                        return mqs.get(hash % mqs.size());
                    }, keys != null ? keys : "default");
                    break;
                case "DELAY":
                    if (delayLevel == null || delayLevel < 1 || delayLevel > 18) {
                        throw new IllegalArgumentException("delayLevel must be between 1 and 18");
                    }
                    msg.setDelayTimeLevel(delayLevel);
                    sendResult = producer.send(msg);
                    break;
                case "TRANSACTION":
                    LocalTransactionState state = Boolean.TRUE.equals(transactionCommit)
                            ? LocalTransactionState.COMMIT_MESSAGE
                            : LocalTransactionState.ROLLBACK_MESSAGE;
                    sendResult = txProducer.sendMessageInTransaction(msg, state);
                    break;
                default:
                    sendResult = producer.send(msg);
                    break;
            }

            log.info("Message sent: topic={}, type={}, msgId={}, queueId={}",
                    topic, messageType, sendResult.getMsgId(), sendResult.getMessageQueue().getQueueId());

            result.put("success", true);
            result.put("msgId", sendResult.getMsgId());
            result.put("queueId", sendResult.getMessageQueue().getQueueId());
            result.put("queueOffset", sendResult.getQueueOffset());
        } catch (Exception e) {
            log.error("Failed to send message: topic={}, type={}", topic, messageType, e);
            result.put("success", false);
            result.put("error", e.getMessage());
        }
        return result;
    }
}
