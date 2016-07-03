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

public class PayCountBolt implements IRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(PayCountBolt.class);
    OutputCollector collector;
    private ConcurrentHashMap<Long, Double> PCMap = null;
    private ConcurrentHashMap<Long, Double> WirelessMap = null;

    @Override
    public void execute(Tuple tuple) {
        Object obj = tuple.getValue(0);
        PaymentMessage message = (PaymentMessage) obj;
        Long timestamp = message.getCreateTime() / (60 * 1000) * 60;
        Double sum = null;
        int platform = 0;
        if (message.getPayPlatform() == 0) {    // PC
            Double total = PCMap.get(timestamp);
            if (total == null) {
                total = 0.0;
                sum = PCMap.get(timestamp - 120L);
                PCMap.remove(timestamp - 180L);
                platform = 0;
                //
            }
            PCMap.put(timestamp, message.getPayAmount() + total);
        } else if (message.getPayPlatform() == 1) { // Wireless
            Double total = WirelessMap.get(timestamp);
            if (total == null) {
                total = 0.0;
                sum = WirelessMap.get(timestamp - 120L);
                WirelessMap.remove(timestamp - 180L);
                platform = 1;
            }
            WirelessMap.put(timestamp, message.getPayAmount() + total);
        }
        if (sum != null) {
            this.collector.emit(new Values(new SumMessage(timestamp, platform, sum)));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(RaceConfig.FIELD_PAY_SUM));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.PCMap = new ConcurrentHashMap<>();
        this.WirelessMap = new ConcurrentHashMap<>();
    }

    @Override
    public void cleanup() {
        // TODO 该类的流程完成后的清理操作，supervisor会执行 kill -9 的操作，因此并不能保证会执行
        Iterator<Map.Entry<Long, Double>> iteratorPC = PCMap.entrySet().iterator();
        while (iteratorPC.hasNext()) {
            Map.Entry<Long, Double> map = iteratorPC.next();
            LOG.info(">>> PC端：" + map.getKey() + "\t-->\t" + map.getValue());
        }

        Iterator<Map.Entry<Long, Double>> iteratorWireless = WirelessMap.entrySet().iterator();
        while (iteratorWireless.hasNext()) {
            Map.Entry<Long, Double> map = iteratorWireless.next();
            LOG.info(">>> 无线端：" + map.getKey() + "\t-->\t" + map.getValue());
        }

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}