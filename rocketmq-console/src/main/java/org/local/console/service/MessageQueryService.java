package org.local.console.service;

import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.*;

@Service
public class MessageQueryService {

    private static final Logger log = LoggerFactory.getLogger(MessageQueryService.class);

    private final DefaultMQAdminExt adminExt;

    public MessageQueryService(DefaultMQAdminExt adminExt) {
        this.adminExt = adminExt;
    }

    public Map<String, Object> queryByKey(String topic, String key, long beginTime, long endTime, int maxNum) {
        Map<String, Object> result = new HashMap<>();
        try {
            QueryResult queryResult = adminExt.queryMessage(topic, key, maxNum, beginTime, endTime);
            List<Map<String, Object>> messages = new ArrayList<>();

            for (MessageExt msg : queryResult.getMessageList()) {
                messages.add(toMessageMap(msg));
            }

            result.put("success", true);
            result.put("messages", messages);
            result.put("total", messages.size());
            result.put("maxNum", maxNum);
        } catch (Exception e) {
            log.error("Failed to query messages by key: topic={}, key={}", topic, key, e);
            result.put("success", false);
            result.put("error", e.getMessage());
        }
        return result;
    }

    public Map<String, Object> queryByMsgId(String topic, String msgId) {
        Map<String, Object> result = new HashMap<>();
        try {
            MessageExt msg = adminExt.viewMessage(topic, msgId);
            if (msg == null) {
                result.put("success", false);
                result.put("error", "Message not found");
            } else {
                result.put("success", true);
                result.put("messages", List.of(toMessageMap(msg)));
                result.put("total", 1);
            }
        } catch (Exception e) {
            log.error("Failed to query message by msgId: topic={}, msgId={}", topic, msgId, e);
            result.put("success", false);
            result.put("error", e.getMessage());
        }
        return result;
    }

    public Map<String, Object> queryByTime(String topic, long beginTime, long endTime, int maxNum) {
        return queryByKey(topic, "*", beginTime, endTime, maxNum);
    }

    private Map<String, Object> toMessageMap(MessageExt msg) {
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
        m.put("bornHost", String.valueOf(msg.getBornHost()));
        m.put("storeHost", String.valueOf(msg.getStoreHost()));
        m.put("reconsumeTimes", msg.getReconsumeTimes());
        m.put("msgSize", msg.getBody().length);
        return m;
    }
}
