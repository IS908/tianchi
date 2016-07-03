package com.alibaba.middleware.race.jstorm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.rocketmq.PaymentConsumer;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.exception.MQClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class RacePaymentMessageSpout implements IRichSpout {
    private static final long serialVersionUID = 2586322232344555988L;
    private static Logger LOG = LoggerFactory.getLogger(RacePaymentMessageSpout.class);

    SpoutOutputCollector collector;
    private BlockingQueue<PaymentMessage> queue = new LinkedBlockingDeque<>();
    private DefaultMQPushConsumer consumer;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        try {
            this.consumer = PaymentConsumer.getConsumer(RaceConfig.MqPayTopic, this.queue);
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
                this.collector.emit("", new Values(paymentMessage));
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
        declarer.declare(new Fields(RaceConfig.FIELD_PAY_DATA));
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