package org.local.console.service;

import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.TopicList;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class ClusterService {

    private static final Logger log = LoggerFactory.getLogger(ClusterService.class);

    private final DefaultMQAdminExt adminExt;

    @Value("${rocketmq.broker-addr:127.0.0.1:10911}")
    private String brokerAddrOverride;

    public ClusterService(DefaultMQAdminExt adminExt) {
        this.adminExt = adminExt;
    }

    private String resolveBrokerAddr() throws Exception {
        ClusterInfo clusterInfo = adminExt.examineBrokerClusterInfo();
        String addr = clusterInfo.getBrokerAddrTable().values().stream()
                .flatMap(bd -> bd.getBrokerAddrs().values().stream())
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No broker available"));
        // Replace Docker internal IP (172.x.x.x) with host-mapped address
        if (addr.startsWith("172.")) {
            log.info("Replacing Docker broker addr {} with {}", addr, brokerAddrOverride);
            return brokerAddrOverride;
        }
        return addr;
    }

    public Map<String, Object> getClusterInfo() {
        Map<String, Object> result = new HashMap<>();
        try {
            ClusterInfo clusterInfo = adminExt.examineBrokerClusterInfo();
            result.put("connected", true);
            result.put("namesrvAddr", adminExt.getNamesrvAddr());
            result.put("clusterNames", clusterInfo.getClusterAddrTable().keySet());
            result.put("brokerCount", clusterInfo.getBrokerAddrTable().size());
        } catch (Exception e) {
            result.put("connected", false);
            result.put("error", e.getMessage());
        }
        return result;
    }

    public Map<String, Object> getBrokers() {
        Map<String, Object> result = new HashMap<>();
        try {
            ClusterInfo clusterInfo = adminExt.examineBrokerClusterInfo();

            List<Map<String, Object>> details = clusterInfo.getBrokerAddrTable()
                    .entrySet().stream()
                    .map(entry -> {
                        Map<String, Object> broker = new HashMap<>();
                        broker.put("cluster", entry.getValue().getCluster());
                        broker.put("brokerName", entry.getValue().getBrokerName());
                        broker.put("addresses", entry.getValue().getBrokerAddrs().values().stream()
                                .map(Object::toString).collect(Collectors.toList()));
                        return broker;
                    }).collect(Collectors.toList());

            result.put("details", details);
        } catch (Exception e) {
            result.put("error", e.getMessage());
        }
        return result;
    }

    public Map<String, Object> getTopics() {
        Map<String, Object> result = new HashMap<>();
        try {
            TopicList topicList = adminExt.fetchAllTopicList();
            Set<String> topics = topicList.getTopicList();
            result.put("topics", topics);
            result.put("count", topics.size());
        } catch (Exception e) {
            result.put("error", e.getMessage());
        }
        return result;
    }

    public Map<String, Object> createTopic(String topic, int queueNum) {
        Map<String, Object> result = new HashMap<>();
        try {
            String brokerAddr = resolveBrokerAddr();
            TopicConfig topicConfig = new TopicConfig(topic, queueNum, queueNum, 6);
            adminExt.createAndUpdateTopicConfig(brokerAddr, topicConfig);
            log.info("Topic created: {} (broker: {}, queueNum: {})", topic, brokerAddr, queueNum);
            result.put("success", true);
            result.put("topic", topic);
        } catch (Exception e) {
            log.error("Failed to create topic: {}", topic, e);
            result.put("success", false);
            result.put("error", e.getMessage());
        }
        return result;
    }

    public Map<String, Object> getTopicStats(String topic) {
        Map<String, Object> result = new HashMap<>();
        try {
            var stats = adminExt.examineTopicStats(topic);
            result.put("topic", topic);
            result.put("offsetTable", stats.getOffsetTable());
        } catch (Exception e) {
            result.put("error", e.getMessage());
        }
        return result;
    }

    public Map<String, Object> healthCheck() {
        Map<String, Object> result = new HashMap<>();
        result.put("status", "UP");
        try {
            adminExt.examineBrokerClusterInfo();
            result.put("rocketmq", "connected");
        } catch (Exception e) {
            result.put("rocketmq", "disconnected");
            result.put("rocketmqError", e.getMessage());
        }
        return result;
    }
}
