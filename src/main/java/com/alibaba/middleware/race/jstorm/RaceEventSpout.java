package com.alibaba.middleware.race.jstorm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

public class RaceEventSpout implements IRichSpout {
    private static Logger LOG = LoggerFactory.getLogger(RaceEventSpout.class);

    SpoutOutputCollector _collector;

    DefaultMQPushConsumer consumer;

    private static Random rand = new Random();


    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;

    }

    @Override
    public void nextTuple() {
        final int platform = rand.nextInt(2);
        final OrderMessage orderMessage = ( platform == 0 ? OrderMessage.createTbaoMessage() : OrderMessage.createTmallMessage());
        orderMessage.setCreateTime(System.currentTimeMillis());
        PaymentMessage[] paymentMessages = PaymentMessage.createPayMentMsg(orderMessage);
        LOG.debug(">>>>>> execute method nextTuple()");
        for (PaymentMessage tmp : paymentMessages) {
            this._collector.emit(new Values(tmp));
        }
    }

    @Override
    public void ack(Object id) {
        LOG.debug(">>>>>> execute method ack()");
        // Ignored
    }

    @Override
    public void fail(Object id) {
        LOG.debug(">>>>>> execute method fail()");
        _collector.emit(new Values(id), id);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        LOG.debug(">>>>>> execute method declareOutputFields()");
        declarer.declare(new Fields(RaceConfig.SPOUT_FILED_ID));
    }

    @Override
    public void close() {
        LOG.debug(">>>>>> execute method close()");
        // TODO Auto-generated method stub

    }

    @Override
    public void activate() {
        LOG.debug(">>>>>> execute method activate()");
        // TODO Auto-generated method stub

    }

    @Override
    public void deactivate() {
        LOG.debug(">>>>>> execute method deactivate()");
        // TODO Auto-generated method stub

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        LOG.debug(">>>>>> execute method getComponentConfiguration()");
        // TODO Auto-generated method stub
        return null;
    }
}