package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.model.SumMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by kevin on 16-7-3.
 */
public class PayCountPCBolt implements IRichBolt {
    private static final long serialVersionUID = -7248704090112987110L;
    private static Logger LOG = LoggerFactory.getLogger(PayCountPCBolt.class);
    private OutputCollector collector;
    private ConcurrentHashMap<Long, Double> pcMap = null;
    private final int platform = 0;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        pcMap = new ConcurrentHashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        Object obj = tuple.getValue(0);
        PaymentMessage message = (PaymentMessage) obj;
        Long timestamp = message.getCreateTime() / (60 * 1000) * 60;
        Double total = pcMap.get(timestamp);
        if (total == null) {
            total = 0.0;
            Double sum = pcMap.get(timestamp - 120L);
            if (sum != null) {
                this.collector.emit(new Values(new SumMessage(timestamp, platform, sum)));
                pcMap.remove(timestamp - 180L);
            }
        }
        pcMap.put(timestamp, message.getPayAmount() + total);
    }

    @Override
    public void cleanup() {
        // 该类的流程完成后的清理操作，supervisor会执行 kill -9 的操作，因此并不能保证会执行
        Iterator<Map.Entry<Long, Double>> iteratorPC = pcMap.entrySet().iterator();
        while (iteratorPC.hasNext()) {
            Map.Entry<Long, Double> map = iteratorPC.next();
            this.collector.emit(new Values(new SumMessage(map.getKey(), platform, map.getValue())));
            LOG.info("PC_{}:{}", map.getKey(), map.getValue());
            this.pcMap.remove(map.getKey());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(RaceConfig.FIELD_PAY_SUM));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
