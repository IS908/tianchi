package com.alibaba.middleware.race.jstorm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.RaceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.Util;

import java.util.Map;
import java.util.Random;

public class RaceSentenceSpout implements IRichSpout {
    private static Logger LOG = LoggerFactory.getLogger(RaceSentenceSpout.class);

    SpoutOutputCollector _collector;

    private int index = 0;
    private static final String[] CHOICES = {
            "marry had a little lamb whos fleese was white as snow",
            "and every where that marry went the lamb was sure to go",
            "one two three four five six seven eight nine ten",
            "this is a test of the emergency broadcast system this is only a test",
            "peter piper picked a peck of pickeled peppers"};

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        LOG.debug(">>>>>> execute method open()");
        _collector = collector;
    }

    @Override
    public void nextTuple() {
        LOG.debug(">>>>>> execute method nextTuple()");
        this._collector.emit(new Values(CHOICES[index]));
        index++;
        if (index >= CHOICES.length) {
            index = 0;
        }
        Utils.sleep(2000);
    }

    @Override
    public void ack(Object id) {
        LOG.debug(">>>>>> execute method ack()");
        // Ignored
    }

    @Override
    public void fail(Object id) {
        LOG.debug(">>>>>> execute method fail()");
        _collector.emit(new Values(id), id);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        LOG.debug(">>>>>> execute method declareOutputFields()");
        declarer.declare(new Fields(RaceConfig.SPOUT_FILED_ID));
    }

    @Override
    public void close() {
        LOG.debug(">>>>>> execute method close()");
        // TODO Auto-generated method stub

    }

    @Override
    public void activate() {
        LOG.debug(">>>>>> execute method activate()");
        // TODO Auto-generated method stub

    }

    @Override
    public void deactivate() {
        LOG.debug(">>>>>> execute method deactivate()");
        // TODO Auto-generated method stub

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        LOG.debug(">>>>>> execute method getComponentConfiguration()");
        // TODO Auto-generated method stub
        return null;
    }
}