package org.local.console.controller;

import org.local.console.service.MessageService;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api")
@CrossOrigin
public class MessageController {

    private final MessageService messageService;

    public MessageController(MessageService messageService) {
        this.messageService = messageService;
    }

    @PostMapping("/message/send")
    public Map<String, Object> sendMessage(@RequestBody Map<String, Object> body) {
        String topic = (String) body.get("topic");
        String tags = (String) body.get("tags");
        String keys = (String) body.get("keys");
        String messageBody = (String) body.get("body");
        String messageType = (String) body.get("messageType");
        Integer delayLevel = body.get("delayLevel") != null
                ? ((Number) body.get("delayLevel")).intValue() : null;
        Boolean transactionCommit = body.get("transactionCommit") != null
                ? (Boolean) body.get("transactionCommit") : null;

        return messageService.sendMessage(topic, tags, keys, messageBody,
                messageType, delayLevel, transactionCommit);
    }
}
