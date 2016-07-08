package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConstant;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.SumMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by kevin on 16-7-3.
 */
public class BoltMergeMessage implements IRichBolt {
    private static final long serialVersionUID = 7124715562791604109L;
    private static Logger LOG = LoggerFactory.getLogger(BoltMergeMessage.class);
    OutputCollector collector;
    private HashMap<Long, Double> orderMap = new HashMap<>();
    private HashMap<Long, Double> payMap = new HashMap<>();

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String streamId = tuple.getSourceStreamId();
        if (streamId.equals(RaceConstant.STREAM_ORDER_PLATFORM)) {
            // 此处进行根据 OrderId 配对订单记录和支付记录

        } else if (streamId.equals(RaceConstant.STREAM_PAY_PLATFORM)) {

        } else if (streamId.equals(RaceConstant.STREAM_STOP)) {
            // 消息结束标志处理
        }

    }

    @Override
    public void cleanup() {
        // TODO 该类的流程完成后的清理操作，supervisor会执行 kill -9 的操作，因此并不能保证会执行
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(RaceConstant.STREAM_PLATFORM_TB,
                new Fields(RaceConstant.payTime, RaceConstant.payAmount));
        declarer.declareStream(RaceConstant.STREAM_PLATFORM_TM,
                new Fields(RaceConstant.payTime, RaceConstant.payAmount));

//        declarer.declare(new Fields(RaceConstant.FIELD_ORDER_SUM));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
