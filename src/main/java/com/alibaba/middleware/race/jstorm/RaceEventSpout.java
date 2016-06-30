package com.alibaba.middleware.race.jstorm;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class RaceEventSpout implements IRichSpout {
    private static Logger LOG = LoggerFactory.getLogger(RaceEventSpout.class);

    SpoutOutputCollector _collector;
    private static Random rand;
    private OrderMessage orderMessage;
    private BlockingQueue<PaymentMessage> queue = new LinkedBlockingDeque<>();

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        try {
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("41413few7x");
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            // TODO 修改name server
            consumer.setNamesrvAddr("127.0.0.1:9876");

            consumer.subscribe(RaceConfig.MqPayTopic, "*");

            consumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                                ConsumeConcurrentlyContext context) {
                    for (MessageExt msg : msgs) {
                        byte [] body = msg.getBody();
                        if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                            //Info: 生产者停止生成数据, 并不意味着马上结束
                            System.out.println("Got the end signal");
                            continue;
                        }
                        PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
                        queue.offer(paymentMessage);
                        System.out.println(paymentMessage);
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });

            consumer.start();
        } catch (Exception e) {

        }

    }

    @Override
    public void nextTuple() {
//        orderMessage.setCreateTime(System.currentTimeMillis());
//        PaymentMessage[] paymentMessages = PaymentMessage.createPayMentMsg(orderMessage);
//        for (PaymentMessage tmp : paymentMessages) {
//            this._collector.emit(new Values(tmp));
//        }
        if (queue.isEmpty()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            PaymentMessage paymentMessage = queue.poll();
            if (paymentMessage != null) {
                this._collector.emit(new Values(paymentMessage));
            }
        }

    }

    @Override
    public void ack(Object id) {
        // Ignored
    }

    @Override
    public void fail(Object id) {
        _collector.emit(new Values(id), id);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(RaceConfig.Field_DataSource));
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        this.rand = new Random();
        int platform = rand.nextInt(2);
        orderMessage = ( platform == 0 ? OrderMessage.createTbaoMessage() : OrderMessage.createTmallMessage());
        return null;
    }
}