package org.local.console.controller;

import org.local.console.service.SimulationService;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/simulation")
@CrossOrigin
public class SimulationController {

    private final SimulationService simulationService;

    public SimulationController(SimulationService simulationService) {
        this.simulationService = simulationService;
    }

    @PostMapping("/producer/start")
    public Map<String, Object> startProducer(@RequestBody Map<String, Object> body) {
        String topic = (String) body.get("topic");
        String tags = (String) body.get("tags");
        String keys = (String) body.get("keys");
        String messageType = body.get("messageType") != null ? (String) body.get("messageType") : "NORMAL";
        Integer delayLevel = body.get("delayLevel") != null
                ? ((Number) body.get("delayLevel")).intValue() : null;
        Boolean transactionCommit = body.get("transactionCommit") != null
                ? (Boolean) body.get("transactionCommit") : null;
        int intervalMs = body.get("intervalMs") != null
                ? ((Number) body.get("intervalMs")).intValue() : 1000;
        long totalMaxCount = body.get("totalMaxCount") != null
                ? ((Number) body.get("totalMaxCount")).longValue() : 0;
        long maxCount = body.get("maxCount") != null
                ? ((Number) body.get("maxCount")).longValue() : 0;
        if (body.get("runMaxCount") != null) {
            maxCount = ((Number) body.get("runMaxCount")).longValue();
        }
        String bodyTemplate = (String) body.get("bodyTemplate");

        return simulationService.startProducer(topic, tags, keys, messageType,
                delayLevel, transactionCommit, intervalMs, totalMaxCount, maxCount, bodyTemplate);
    }

    @PostMapping("/producer/stop")
    public Map<String, Object> stopProducer(@RequestBody Map<String, Object> body) {
        String taskId = (String) body.get("taskId");
        return simulationService.stopTask(taskId);
    }

    @PostMapping("/producer/restart")
    public Map<String, Object> restartProducer(@RequestBody Map<String, Object> body) {
        String taskId = (String) body.get("taskId");
        return simulationService.restartProducer(taskId);
    }

    @PostMapping("/consumer/start")
    public Map<String, Object> startConsumer(@RequestBody Map<String, Object> body) {
        String topic = (String) body.get("topic");
        String consumerGroup = (String) body.get("consumerGroup");
        String tagFilter = (String) body.get("tagFilter");
        int pollIntervalMs = body.get("pollIntervalMs") != null
                ? ((Number) body.get("pollIntervalMs")).intValue() : 1000;

        return simulationService.startConsumer(topic, consumerGroup, tagFilter, pollIntervalMs);
    }

    @PostMapping("/consumer/stop")
    public Map<String, Object> stopConsumer(@RequestBody Map<String, Object> body) {
        String taskId = (String) body.get("taskId");
        return simulationService.stopTask(taskId);
    }

    @PostMapping("/consumer/restart")
    public Map<String, Object> restartConsumer(@RequestBody Map<String, Object> body) {
        String taskId = (String) body.get("taskId");
        return simulationService.restartConsumer(taskId);
    }

    @GetMapping("/tasks")
    public List<Map<String, Object>> tasks() {
        return simulationService.getTasks();
    }

    @PostMapping("/stop-all")
    public Map<String, Object> stopAll() {
        return simulationService.stopAll();
    }

    @PostMapping("/task/remove")
    public Map<String, Object> removeTask(@RequestBody Map<String, Object> body) {
        String taskId = (String) body.get("taskId");
        return simulationService.removeTask(taskId);
    }

    @GetMapping("/task/{taskId}/messages")
    public List<Map<String, Object>> taskMessages(@PathVariable String taskId,
                                                   @RequestParam(defaultValue = "200") int limit) {
        return simulationService.getMessages(taskId, limit);
    }

    @GetMapping("/messages")
    public List<Map<String, Object>> allMessages(@RequestParam(defaultValue = "0") int offset,
                                                  @RequestParam(defaultValue = "200") int limit) {
        return simulationService.getAllMessages(offset, limit);
    }

    @GetMapping("/messages/count")
    public Map<String, Object> messageCount() {
        return Map.of("total", simulationService.getTotalMessageCount());
    }

    @GetMapping("/producers")
    public List<Map<String, Object>> producerHistory() {
        return simulationService.getProducerHistory();
    }

    @GetMapping("/producer/{taskId}")
    public Map<String, Object> producerDetail(@PathVariable String taskId) {
        return simulationService.getProducerDetail(taskId);
    }

    @GetMapping("/consumer/{taskId}")
    public Map<String, Object> consumerDetail(@PathVariable String taskId) {
        return simulationService.getConsumerDetail(taskId);
    }

    @GetMapping("/consumer/{taskId}/messages")
    public Map<String, Object> consumerMessages(@PathVariable String taskId,
                                                 @RequestParam(defaultValue = "0") int offset,
                                                 @RequestParam(defaultValue = "50") int limit) {
        List<Map<String, Object>> msgs = simulationService.getConsumerMessages(taskId, offset, limit);
        long total = simulationService.getConsumerMessageCount(taskId);
        return Map.of("messages", msgs, "total", total, "offset", offset, "limit", limit);
    }

    @GetMapping("/producer/{taskId}/messages")
    public Map<String, Object> producerMessages(@PathVariable String taskId,
                                                 @RequestParam(defaultValue = "0") int offset,
                                                 @RequestParam(defaultValue = "50") int limit) {
        List<Map<String, Object>> msgs = simulationService.getProducerMessages(taskId, offset, limit);
        long total = simulationService.getProducerMessageCount(taskId);
        return Map.of("messages", msgs, "total", total, "offset", offset, "limit", limit);
    }
}
