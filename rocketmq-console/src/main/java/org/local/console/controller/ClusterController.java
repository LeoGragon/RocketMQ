package org.local.console.controller;

import org.local.console.service.ClusterService;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api")
@CrossOrigin
public class ClusterController {

    private final ClusterService clusterService;

    public ClusterController(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @GetMapping("/cluster")
    public Map<String, Object> clusterInfo() {
        return clusterService.getClusterInfo();
    }

    @GetMapping("/brokers")
    public Map<String, Object> brokers() {
        return clusterService.getBrokers();
    }

    @GetMapping("/topics")
    public Map<String, Object> topics() {
        return clusterService.getTopics();
    }

    @GetMapping("/topic/{topic}/stats")
    public Map<String, Object> topicStats(@PathVariable String topic) {
        return clusterService.getTopicStats(topic);
    }

    @PostMapping("/topic")
    public Map<String, Object> createTopic(@RequestBody Map<String, Object> body) {
        String topic = (String) body.get("topic");
        int queueNum = body.get("queueNum") != null ? ((Number) body.get("queueNum")).intValue() : 8;
        return clusterService.createTopic(topic, queueNum);
    }

    @GetMapping("/health")
    public Map<String, Object> health() {
        return clusterService.healthCheck();
    }
}
