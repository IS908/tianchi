package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;
import com.esotericsoftware.minlog.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class SplitSentence implements IRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(SplitSentence.class);

    OutputCollector collector;

    @Override
    public void execute(Tuple tuple) {
        LOG.debug(">>>>>> execute method execute()");
        String sentence = tuple.getStringByField(RaceConfig.SPOUT_FILED_ID);
        String[] words = sentence.split(" ");
        for (String word : words) {
            this.collector.emit(new Values(word));
        }
        /*String sentence = tuple.getString(0);
        for (String word : sentence.split("\\s+")) {
            collector.emit(new Values(word));
        }
        collector.ack(tuple);*/
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        LOG.debug(">>>>>> execute method declareOutputFields()");
        declarer.declare(new Fields(RaceConfig.BOLT_FILED_ID));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        LOG.debug(">>>>>> execute method prepare()");
        this.collector = collector;
    }

    @Override
    public void cleanup() {
        LOG.debug(">>>>>> execute method cleanup()");
        // TODO Auto-generated method stub

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Log.debug(">>>>>> execute method getComponentConfiguration()");
        // TODO Auto-generated method stub
        return null;
    }
}
