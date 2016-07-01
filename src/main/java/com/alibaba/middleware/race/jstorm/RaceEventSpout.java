package com.alibaba.middleware.race.jstorm;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.rocketmq.Consumer;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.exception.MQClientException;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class RaceEventSpout implements IRichSpout {
    private static Logger LOG = LoggerFactory.getLogger(RaceEventSpout.class);

    SpoutOutputCollector collector;
    private BlockingQueue<PaymentMessage> queue = new LinkedBlockingDeque<>();
    private DefaultMQPushConsumer consumer;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        try {
            this.consumer = Consumer.getConsumer(this.queue);
        } catch (MQClientException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
        }

    }

    @Override
    public void nextTuple() {
        if (queue.isEmpty()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            PaymentMessage paymentMessage = queue.poll();
            if (paymentMessage != null) {
                this.collector.emit(new Values(paymentMessage));
            }
        }

    }

    @Override
    public void ack(Object id) {
        // Ignored
    }

    @Override
    public void fail(Object id) {
        consumer.shutdown();
        collector.emit(new Values(id), id);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(RaceConfig.Field_DataSource));
    }

    @Override
    public void close() {
        consumer.shutdown();
    }

    @Override
    public void activate() {
        consumer.resume();
    }

    @Override
    public void deactivate() {
        consumer.suspend();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}