package com.alibaba.middleware.race.jstorm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.rocketmq.OrderConsumer;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.exception.MQClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by kevin on 16-7-3.
 */
public class RaceOrderMessageSpout implements IRichSpout {
    private static final long serialVersionUID = 902356590300594458L;
    private static Logger LOG = LoggerFactory.getLogger(RaceOrderMessageSpout.class);

    SpoutOutputCollector collector;
    private BlockingQueue<OrderMessage> tbQueue = new LinkedBlockingDeque<>();
    private BlockingQueue<OrderMessage> tmQueue = new LinkedBlockingDeque<>();
    private DefaultMQPushConsumer[] consumers;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        try {
            this.consumers = OrderConsumer.getConsumer(this.tbQueue, this.tmQueue);
        } catch (MQClientException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
        }
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

    }

    @Override
    public void fail(Object msgId) {
        for (DefaultMQPushConsumer consumer : consumers) {
            consumer.shutdown();
        }
        collector.emit(new Values(msgId), msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
