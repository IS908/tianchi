package com.alibaba.middleware.race.jstorm.trident;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.model.PaymentMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.spout.ITridentSpout;

import java.util.Map;

/**
 * 拉取RocketMQ中的数据
 * Created by kevin on 16-6-29.
 */
public class RocketMQEventSpout implements ITridentSpout<PaymentMessage> {
    private static final long serialVersionUID = -5258461915262842740L;
    private static final Logger LOG = LoggerFactory.getLogger(RocketMQEventSpout.class);

    SpoutOutputCollector collector;
    BatchCoordinator<PaymentMessage> coordinator = new DefaultCoordinator();
    Emitter<PaymentMessage> emitter = new SpoutEventEmitter();

    @Override
    public BatchCoordinator<PaymentMessage> getCoordinator(String txStateId, Map conf, TopologyContext context) {
        LOG.info(">>>>>> getCoordinator() execute!");
        return this.coordinator;
    }

    @Override
    public Emitter<PaymentMessage> getEmitter(String txStateId, Map conf, TopologyContext context) {
        LOG.info(">>>>>> getEmitter() execute!");
        return this.emitter;
    }

    @Override
    public Map getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        LOG.info(">>>>>> getOutputFields() execute!");
        return new Fields(RaceConfig.ID_SPLIT_PLATFORM);
    }
}
