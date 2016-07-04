package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.SumMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by kevin on 16-7-4.
 */
public class OrderCountTBBolt implements IRichBolt {
    private static final long serialVersionUID = -5501370330879354521L;
    private static Logger LOG = LoggerFactory.getLogger(OrderCountBolt.class);
    OutputCollector collector;
    private ConcurrentHashMap<Long, Double> tbMap = null;
    private final int platform = 0;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.tbMap = new ConcurrentHashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        Object obj = tuple.getValue(0);
        OrderMessage message = (OrderMessage) obj;
//        LOG.info("ordermessage={}", message);
        Long timestamp = message.getCreateTime() / (60 * 1000) * 60;
        Double sum = null;
        // 淘宝平台订单
        Double total = tbMap.get(timestamp);
        if (total == null) {
            total = 0.0;
            sum = tbMap.get(timestamp - 120L);
            tbMap.remove(timestamp - 180L);
        }
        total += message.getTotalPrice();
        tbMap.put(timestamp, total);

        if (sum != null) {
            collector.emit(new Values(new SumMessage(timestamp, platform, sum)));
        }
    }

    @Override
    public void cleanup() {
        // 该类的流程完成后的清理操作，supervisor会执行 kill -9 的操作，因此并不能保证会执行
        Iterator<Map.Entry<Long, Double>> iteratorPC = tbMap.entrySet().iterator();
        while (iteratorPC.hasNext()) {
            Map.Entry<Long, Double> map = iteratorPC.next();
            this.collector.emit(new Values(new SumMessage(map.getKey(), platform, map.getValue())));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(RaceConfig.FIELD_ORDER_SUM));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
