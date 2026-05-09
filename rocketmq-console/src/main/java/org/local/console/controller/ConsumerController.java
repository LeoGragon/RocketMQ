package org.local.console.controller;

import org.local.console.service.ConsumerService;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/consumer")
@CrossOrigin
public class ConsumerController {

    private final ConsumerService consumerService;

    public ConsumerController(ConsumerService consumerService) {
        this.consumerService = consumerService;
    }

    @PostMapping("/start")
    public Map<String, Object> start(@RequestBody Map<String, Object> body) {
        String topic = (String) body.get("topic");
        String consumerGroup = (String) body.get("consumerGroup");
        String tagFilter = (String) body.get("tagFilter");
        return consumerService.start(topic, consumerGroup, tagFilter);
    }

    @PostMapping("/poll")
    public Map<String, Object> poll(@RequestBody Map<String, Object> body) {
        String sessionKey = (String) body.get("sessionKey");
        return consumerService.poll(sessionKey);
    }

    @PostMapping("/stop")
    public Map<String, Object> stop(@RequestBody Map<String, Object> body) {
        String sessionKey = (String) body.get("sessionKey");
        return consumerService.stop(sessionKey);
    }

    @GetMapping("/sessions")
    public java.util.List<Map<String, Object>> sessions() {
        return consumerService.listSessions();
    }
}
