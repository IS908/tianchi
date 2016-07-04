package com.alibaba.middleware.race.jstorm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.rocketmq.Consumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Zheng Kun on 2016/7/3.
 */
public class RocketMqSpout implements IRichSpout, MessageListenerConcurrently {
    private static final Logger LOGGER = LoggerFactory.getLogger(RocketMqSpout.class);
    private ConcurrentHashMap<UUID, Values> pending;

    private SpoutOutputCollector collector;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.pending = new ConcurrentHashMap<>();
        Consumer.registerListener(this);
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
    public void nextTuple() {

    }

    @Override
    public void ack(Object msgId) {
        this.pending.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        this.collector.emit(this.pending.get(msgId), msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(RaceConfig.FIELD_SOURCE_DATA));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        for (MessageExt msg : msgs) {
            UUID uuid = UUID.randomUUID();
            byte[] body = msg.getBody();
            if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                //Info: 生产者停止生成数据, 并不意味着马上结束
                System.out.println("Got the end signal");
                continue;
            }
            String topic = msg.getTopic();

            // 付款消息
            if (RaceConfig.MqPayTopic.equals(topic)) {
                PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
                Values values = new Values(paymentMessage);
                collector.emit(values, uuid);
                this.pending.put(uuid, values);
//                LOGGER.info("topic={}, message={}", topic, JSON.toJSONString(paymentMessage));
            } else if (RaceConfig.MqTaobaoTradeTopic.equals(topic) || RaceConfig.MqTmallTradeTopic.equals(topic)) {
                OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
                Values values = new Values(orderMessage);
                collector.emit(values, uuid);
                this.pending.put(uuid, values);
//                LOGGER.info("topic={}, message={}", topic, JSON.toJSONString(orderMessage));
            }
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
}
