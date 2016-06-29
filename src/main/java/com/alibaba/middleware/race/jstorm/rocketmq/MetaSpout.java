package com.alibaba.middleware.race.jstorm.rocketmq;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import com.alibaba.jstorm.client.spout.IAckValueSpout;
import com.alibaba.jstorm.client.spout.IFailValueSpout;
import com.alibaba.jstorm.metric.MetricClient;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by kevin on 16-6-29.
 */
public class MetaSpout implements IRichSpout, IAckValueSpout, IFailValueSpout,
        MessageListenerConcurrently {

    private static final Logger LOG = Logger.getLogger(MetaSpout.class);

    protected MetaClientConfig metaClientConfig;
    protected SpoutOutputCollector collector;
    /*protected transient MetaPushConsumer consumer;

    protected Map conf;
    protected String id;
    protected boolean flowControl;
    protected boolean autoAck;

    protected transient LinkedBlockingDeque<MetaTuple> sendingQueue;

    protected transient MetricClient metricClient;
    protected transient JStormHistogram waithHistogram;
    protected transient JStormHistogram processHistogram;

    public MetaSpout() {

    }

    public void initMetricClient(TopologyContext context) {
        metricClient = new MetricClient(context);
        waithHistogram = metricClient.registerHistogram("MetaTupleWait", null);
        processHistogram = metricClient.registerHistogram("MetaTupleProcess",
                null);
    }*/

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

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
    public void nextTuple() {

    }

    @Override
    public void ack(Object msgId) {

    }

    @Override
    public void fail(Object msgId) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void ack(Object msgId, List<Object> values) {

    }

    @Override
    public void fail(Object msgId, List<Object> values) {

    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        return null;
    }
}
