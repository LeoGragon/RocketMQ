package org.local.console.config;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.*;

@Configuration
public class RocketMQConfig {

    private static final Logger log = LoggerFactory.getLogger(RocketMQConfig.class);

    @Value("${rocketmq.namesrv-addr:127.0.0.1:9876}")
    private String namesrvAddr;

    @Bean
    public DefaultMQAdminExt defaultMQAdminExt() throws Exception {
        DefaultMQAdminExt adminExt = new DefaultMQAdminExt();
        adminExt.setNamesrvAddr(namesrvAddr);
        adminExt.setAdminExtGroup("console-admin-group");
        adminExt.setInstanceName("console-instance-" + System.currentTimeMillis());
        adminExt.start();
        return adminExt;
    }

    @Bean
    public DefaultMQProducer defaultMQProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("console-producer-group");
        producer.setNamesrvAddr(namesrvAddr);
        producer.setInstanceName("console-producer-" + System.currentTimeMillis());
        producer.setSendMsgTimeout(15000);
        producer.setRetryTimesWhenSendFailed(0);
        producer.setRetryTimesWhenSendAsyncFailed(0);
        producer.setVipChannelEnabled(false);
        producer.setSendLatencyFaultEnable(true);
        producer.start();
        return producer;
    }

    @Bean
    public TransactionMQProducer transactionMQProducer() throws Exception {
        TransactionMQProducer producer = new TransactionMQProducer("console-tx-producer-group");
        producer.setNamesrvAddr(namesrvAddr);
        producer.setInstanceName("console-tx-producer-" + System.currentTimeMillis());
        producer.setSendMsgTimeout(15000);
        producer.setRetryTimesWhenSendFailed(0);
        producer.setRetryTimesWhenSendAsyncFailed(0);
        producer.setVipChannelEnabled(false);
        producer.setSendLatencyFaultEnable(true);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        producer.setExecutorService(executor);
        producer.setTransactionListener(new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                if (arg instanceof LocalTransactionState) {
                    return (LocalTransactionState) arg;
                }
                return LocalTransactionState.COMMIT_MESSAGE;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });
        producer.start();
        return producer;
    }
}
