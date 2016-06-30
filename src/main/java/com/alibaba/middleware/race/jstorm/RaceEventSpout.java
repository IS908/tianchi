package com.alibaba.middleware.race.jstorm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

public class RaceEventSpout implements IRichSpout {
    private static Logger LOG = LoggerFactory.getLogger(RaceEventSpout.class);

    SpoutOutputCollector _collector;
    private static Random rand;
    private OrderMessage orderMessage;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void nextTuple() {
        orderMessage.setCreateTime(System.currentTimeMillis());
        PaymentMessage[] paymentMessages = PaymentMessage.createPayMentMsg(orderMessage);
        for (PaymentMessage tmp : paymentMessages) {
            this._collector.emit(new Values(tmp));
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