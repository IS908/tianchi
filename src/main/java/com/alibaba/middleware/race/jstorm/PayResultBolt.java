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

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by kevin on 16-6-26.
 */
public class PayResultBolt implements IRichBolt {
    private static final long serialVersionUID = -1910650485341329191L;
    private static Logger LOG = LoggerFactory.getLogger(PayResultBolt.class);

    private ConcurrentHashMap<Long, Double> PCMap = null;
    private ConcurrentHashMap<Long, Double> WirelessMap = null;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.PCMap = new ConcurrentHashMap<>();
        this.WirelessMap = new ConcurrentHashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        Object obj = tuple.getValue(0);
        SumMessage message = (SumMessage) obj;
        Long timestamp = message.getTimestamp();
        Double pcSum = null;
        Double wirelessSum = null;
        /*
        * 此处的逻辑待完善
        * */
        if (message.getPlatform() == 0) {
            Double pcAccount = PCMap.get(timestamp);
            if (pcAccount == null) {
                pcAccount = 0.0d;
            }
            PCMap.put(timestamp, pcAccount + message.getTotal());
            pcSum = PCMap.get(timestamp - 60L);
        } else {
            Double wirelessAccount = WirelessMap.get(timestamp);
            if (wirelessAccount == null) {
                wirelessAccount = 0.0d;
            }
            WirelessMap.put(timestamp, wirelessAccount + message.getTotal());
            wirelessSum = WirelessMap.get(timestamp - 60L);
        }
        // 执行写tair操作
        if (pcSum != null && wirelessSum != null) {
            TairOperatorImpl.getInstance().write(RaceConfig.prex_ratio + timestamp, wirelessSum / pcSum);
            PCMap.remove(timestamp - 180L);
            WirelessMap.remove(timestamp - 180L);
        }
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
