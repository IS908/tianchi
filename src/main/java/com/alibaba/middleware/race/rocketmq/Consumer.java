/*
 * Copyright (C) 2016 Baidu, Inc. All Rights Reserved.
 */
package com.alibaba.middleware.race.rocketmq;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by v_zhengkun on 2016/7/3.
 */
public class Consumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);
    private static DefaultMQPushConsumer consumer;

    static {
        consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        // TODO 提交测试前注释掉namesrvaddr配置，阿里会自己指定地址及端口
//        consumer.setNamesrvAddr("127.0.0.1:9876");
//        consumer.setNamesrvAddr("192.168.1.10:9876");

        try {
            consumer.subscribe(RaceConfig.MqTmallTradeTopic, SubscriptionData.SUB_ALL);
            consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, SubscriptionData.SUB_ALL);
            consumer.subscribe(RaceConfig.MqPayTopic, SubscriptionData.SUB_ALL);
        } catch (MQClientException e) {
            LOGGER.error("subscribe error{}", e);
        }
    }

    public static void registerListener(MessageListenerConcurrently listener) {
        consumer.registerMessageListener(listener);
        try {
            consumer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }
}
