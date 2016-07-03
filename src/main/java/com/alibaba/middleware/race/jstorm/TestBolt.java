/*
 * Copyright (C) 2016 Baidu, Inc. All Rights Reserved.
 */
package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.fastjson.JSON;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by v_zhengkun on 2016/7/3.
 */
public class TestBolt implements IRichBolt {
    private static final Logger LOGGER  = LoggerFactory.getLogger(TestBolt.class);
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {
        Object message = input.getValue(0);
        if (message instanceof OrderMessage) {
            LOGGER.info("orderMessage = {}", JSON.toJSONString(message));
        } else if (message instanceof PaymentMessage) {
            LOGGER.info("paymentMessage = {}", JSON.toJSONString(message));
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
