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
public class PayCountWirelessBolt implements IRichBolt {
    private static final long serialVersionUID = 8183236198457968724L;
    private static Logger LOG = LoggerFactory.getLogger(PayCountWirelessBolt.class);
    OutputCollector collector;
    private ConcurrentHashMap<Long, Double> wirelessMap = null;
    private final int platform = 1;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.wirelessMap = new ConcurrentHashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        Object obj = tuple.getValue(0);
        PaymentMessage message = (PaymentMessage) obj;
        long current_timestamp = (message.getCreateTime() / (60 * 1000)) * 60;
        long emit_timestamp = current_timestamp - 120L;
        long remove_timestamp = current_timestamp - 240L;

        Double total = wirelessMap.get(current_timestamp);
        if (total == null) {
            total = 0.0;
            Double sum = wirelessMap.get(emit_timestamp);
            if (sum != null) {
                this.collector.emit(new Values(new SumMessage(emit_timestamp, platform, sum)));
            }
//            wirelessMap.remove(remove_timestamp);
        }
        total += message.getPayAmount();
        wirelessMap.put(current_timestamp, total);

        this.collector.ack(tuple);
    }

    @Override
    public void cleanup() {
        // 该类的流程完成后的清理操作，supervisor会执行 kill -9 的操作，因此并不能保证会执行
        Iterator<Map.Entry<Long, Double>> iteratorPC = wirelessMap.entrySet().iterator();
        while (iteratorPC.hasNext()) {
            Map.Entry<Long, Double> map = iteratorPC.next();
            this.collector.emit(new Values(new SumMessage(map.getKey(), platform, map.getValue())));
            LOG.info("Wireless_{}:{}", map.getKey(), map.getValue());
            this.wirelessMap.remove(map.getKey());
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
