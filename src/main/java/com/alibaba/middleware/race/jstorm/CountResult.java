package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import edu.emory.mathcs.backport.java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by kevin on 16-6-26.
 */
public class CountResult implements IRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(CountResult.class);

    private HashMap<String, Integer> counts = null;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        LOG.debug(">>>>>> execute method prepare()");
        this.counts = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        LOG.debug(">>>>>> execute method execute()");
        String word = tuple.getStringByField("word");
        Integer count = tuple.getIntegerByField("count");
        this.counts.put(word, count);
    }

    @Override
    public void cleanup() {
        LOG.debug(">>>>>> execute method cleanup()");
        LOG.info("--- FINAL COUNTS ---");
        List<String> keys = new ArrayList<>();
        keys.addAll(this.counts.keySet());
        Collections.sort(keys);
        for (String key : keys) {
            LOG.info(key + " : " + this.counts.get(key));
        }
        LOG.info("---<<<<<<<<<<<<>>>>>>>>>>>>---");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        LOG.debug(">>>>>> execute method declareOutputFields()");
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        LOG.debug(">>>>>> execute method getComponentConfiguration()");
        return null;
    }
}
