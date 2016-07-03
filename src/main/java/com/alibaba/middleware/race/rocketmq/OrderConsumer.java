package com.alibaba.middleware.race.rocketmq;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

/**
 * Created by kevin on 16-7-3.
 */
public class OrderConsumer {
    public static DefaultMQPushConsumer[] getConsumer(final BlockingQueue<OrderMessage> tbQueue,
                                                      final BlockingQueue<OrderMessage> queue)
            throws MQClientException {
        DefaultMQPushConsumer tb_consumer =
                OrderConsumer.getOrderConsumerByTopic(RaceConfig.MqTaobaoTradeTopic, tbQueue);
        DefaultMQPushConsumer tm_consumer = OrderConsumer.getOrderConsumerByTopic(RaceConfig.MqTmallTradeTopic, queue);
        DefaultMQPushConsumer[] consumers = new DefaultMQPushConsumer[2];
        consumers[0] = tb_consumer;
        consumers[1] = tm_consumer;
        return consumers;
    }

    private static DefaultMQPushConsumer getOrderConsumerByTopic(String topic, final BlockingQueue<OrderMessage> queue)
            throws MQClientException {
        DefaultMQPushConsumer tm_consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);

        tm_consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        // TODO 在本地搭建好broker后,记得指定nameServer的地址
        tm_consumer.setNamesrvAddr(RaceConfig.MQ_NAME_SERVER);

        tm_consumer.subscribe(topic, "*");
        tm_consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    byte[] body = msg.getBody();
                    if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                        //Info: 生产者停止生成数据, 并不意味着马上结束
                        System.out.println("Got the end signal");
                        continue;
                    }
                    OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
                    queue.offer(orderMessage);
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        tm_consumer.start();
        System.out.println("OrderConsumer Started.");
        return tm_consumer;
    }
}
