package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.esotericsoftware.minlog.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;


public class SplitStreamBolt implements IRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(SplitStreamBolt.class);

    OutputCollector collector;

    @Override
    public void execute(Tuple tuple) {
        LOG.debug(">>>>>> execute method execute()");
        List<Object> list = tuple.getValues();
        System.out.println("###### " + list.size());
        for (Object obj : list) {
            PaymentMessage message = (PaymentMessage) obj;
            long timestamp = message.getCreateTime() / 60000 * 60000;
            System.out.println("######" + message);
            Date date = new Date(timestamp);
            System.out.println("######" + date.toString());
        }

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

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Log.debug(">>>>>> execute method getComponentConfiguration()");
        Config conf = new Config();
        return conf;
    }
}
