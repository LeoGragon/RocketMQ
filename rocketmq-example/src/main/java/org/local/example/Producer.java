package org.local.example;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

public class Producer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("example_producer_group");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();

        for (int i = 0; i < 10; i++) {
            String body = "Hello RocketMQ " + i;
            Message msg = new Message("TopicTest", "TagA", body.getBytes());
            SendResult result = producer.send(msg);
            System.out.printf("Sent: %s, result: %s%n", body, result.getSendStatus());
        }

        producer.shutdown();
        System.out.println("Producer shutdown.");
    }
}
