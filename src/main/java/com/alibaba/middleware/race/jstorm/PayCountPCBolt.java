package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by kevin on 16-7-3.
 */
public class PayCountPCBolt implements IRichBolt {
    private static final long serialVersionUID = -7248704090112987110L;
    private static Logger LOG = LoggerFactory.getLogger(PayCountPCBolt.class);
    private  OutputCollector collector;
    private ConcurrentHashMap<Long, Double> map = null;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        map = new ConcurrentHashMap<>();
    }

    @Override
    public void execute(Tuple input) {

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
