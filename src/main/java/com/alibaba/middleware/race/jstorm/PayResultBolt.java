package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.SumMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by kevin on 16-6-26.
 */
public class PayResultBolt implements IRichBolt {
    private static final long serialVersionUID = -1910650485341329191L;
    private static Logger LOG = LoggerFactory.getLogger(PayResultBolt.class);
    private OutputCollector collector;
    private ConcurrentHashMap<Long, Double> PCMap = null;
    private ConcurrentHashMap<Long, Double> WirelessMap = null;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.PCMap = new ConcurrentHashMap<>();
        this.WirelessMap = new ConcurrentHashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        Object obj = tuple.getValue(0);
        SumMessage message = (SumMessage) obj;
        long timestamp = message.getTimestamp();
        long calculate_timestamp = timestamp - 60L;
        long remove_timestamp = timestamp - 240L;

        Double pcSum = null;
        Double wirelessSum = null;

        if (message.getPlatform() == 0) {
            Double pcAccount = PCMap.get(timestamp);
            if (pcAccount == null) {
                pcAccount = 0.0d;
                pcSum = PCMap.get(calculate_timestamp);
            }
            PCMap.put(timestamp, pcAccount + message.getTotal());
        } else {
            Double wirelessAccount = WirelessMap.get(timestamp);
            if (wirelessAccount == null) {
                wirelessAccount = 0.0d;
                wirelessSum = WirelessMap.get(calculate_timestamp);
            }
            WirelessMap.put(timestamp, wirelessAccount + message.getTotal());
        }
        // 执行写tair操作
        if (pcSum != null && wirelessSum != null) {
            String res = String.format("%.2f", wirelessSum / pcSum);
            TairOperatorImpl.getInstance().write(
                    RaceConfig.prex_ratio + calculate_timestamp,
                    res);
            LOG.error("ratio {}:{}", calculate_timestamp, res);

            WirelessMap.put(timestamp, wirelessSum + WirelessMap.get(timestamp));
            PCMap.put(timestamp, pcSum + PCMap.get(timestamp));

//            PCMap.remove(remove_timestamp);
//            WirelessMap.remove(remove_timestamp);
        }

        this.collector.ack(tuple);
    }

    @Override
    public void cleanup() {
        // 关闭前将最后的结果写入 tair 中
        for (Map.Entry<Long, Double> map : PCMap.entrySet()) {
            Double wirelessSum = WirelessMap.get(map.getKey());
            if (wirelessSum != null) {
                TairOperatorImpl.getInstance().write(RaceConfig.prex_ratio + map.getKey(), wirelessSum / map.getValue());
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
