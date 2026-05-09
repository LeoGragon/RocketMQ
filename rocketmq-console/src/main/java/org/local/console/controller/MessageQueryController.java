package org.local.console.controller;

import org.local.console.service.MessageQueryService;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/message")
@CrossOrigin
public class MessageQueryController {

    private final MessageQueryService messageQueryService;

    public MessageQueryController(MessageQueryService messageQueryService) {
        this.messageQueryService = messageQueryService;
    }

    @PostMapping("/queryByKey")
    public Map<String, Object> queryByKey(@RequestBody Map<String, Object> body) {
        String topic = (String) body.get("topic");
        String key = (String) body.get("key");
        long beginTime = body.get("beginTime") != null
                ? ((Number) body.get("beginTime")).longValue() : 0;
        long endTime = body.get("endTime") != null
                ? ((Number) body.get("endTime")).longValue() : System.currentTimeMillis();
        int maxNum = body.get("maxNum") != null
                ? ((Number) body.get("maxNum")).intValue() : 32;
        return messageQueryService.queryByKey(topic, key, beginTime, endTime, maxNum);
    }

    @PostMapping("/queryByMsgId")
    public Map<String, Object> queryByMsgId(@RequestBody Map<String, Object> body) {
        String topic = (String) body.get("topic");
        String msgId = (String) body.get("msgId");
        return messageQueryService.queryByMsgId(topic, msgId);
    }

    @PostMapping("/queryByTime")
    public Map<String, Object> queryByTime(@RequestBody Map<String, Object> body) {
        String topic = (String) body.get("topic");
        long beginTime = body.get("beginTime") != null
                ? ((Number) body.get("beginTime")).longValue() : 0;
        long endTime = body.get("endTime") != null
                ? ((Number) body.get("endTime")).longValue() : System.currentTimeMillis();
        int maxNum = body.get("maxNum") != null
                ? ((Number) body.get("maxNum")).intValue() : 32;
        return messageQueryService.queryByTime(topic, beginTime, endTime, maxNum);
    }
}
