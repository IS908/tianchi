package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class PayCountBolt implements IRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(PayCountBolt.class);

    OutputCollector collector;
    Map<String, Integer> counts;

    @Override
    public void execute(Tuple tuple) {
        LOG.debug(">>>>>> execute method execute()");
        String word = tuple.getStringByField(RaceConfig.BOLT_FILED_ID);
        Integer count = this.counts.get(word);
        if (count == null) {
            count = 0;
        }
        count++;
        this.counts.put(word, count);
        this.collector.emit(new Values(word, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        LOG.debug(">>>>>> execute method declareOutputFields()");
        declarer.declare(new Fields(RaceConfig.BOLT_FILED_ID, RaceConfig.BOLT_COUNT_ID));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        LOG.debug(">>>>>> execute method prepare()");
        this.collector = collector;
        this.counts = new HashMap<>();

    }

    @Override
    public void cleanup() {
        LOG.debug(">>>>>> execute method cleanup()");
        // TODO  该类的流程完成后的清理操作，supervisor会执行 kill -9 的操作，因此并不能保证会执行

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        LOG.debug(">>>>>> execute method getComponentConfiguration()");
        // TODO Auto-generated method stub
        return null;
    }
}