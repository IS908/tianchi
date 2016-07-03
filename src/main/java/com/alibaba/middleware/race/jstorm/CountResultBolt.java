package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.model.PaySum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by kevin on 16-6-26.
 */
public class CountResultBolt implements IRichBolt {
    private static final long serialVersionUID = -1910650485341329191L;
    private static Logger LOG = LoggerFactory.getLogger(CountResultBolt.class);

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
        PaySum message = (PaySum) obj;
        Long timestamp = message.getTimestamp();
        if (message.getPlatform() == 0) {
            Double pcAccount = PCMap.get(timestamp);
            if (pcAccount == null) {
                pcAccount = 0.0d;
            }
            PCMap.put(timestamp, pcAccount + message.getTotal());
            Double res = PCMap.get(timestamp - 60);
            // TODO 执行写入tair操作// TODO
            LOG.info("$$$" + res);
        } else {
            Double wirelessAccount = WirelessMap.get(timestamp);
            if (wirelessAccount == null) {
                wirelessAccount = 0.0d;
            }
            WirelessMap.put(timestamp, wirelessAccount + message.getTotal());
            Double res = WirelessMap.get(timestamp - 60);
            // TODO 执行写入tair操作// TODO
            LOG.info("$$$" + res);
        }
    }

    @Override
    public void cleanup() {
        // TODO 关闭i前将做后的结果写入 tair 中
        Iterator<Map.Entry<Long, Double>> iteratorPC = PCMap.entrySet().iterator();
        while (iteratorPC.hasNext()) {
            Map.Entry<Long, Double> map = iteratorPC.next();
            // TODO 执行写入tair操作// TODO
            LOG.info(">>> PC端：" + map.getKey() + "\t-->\t" + map.getValue());
        }

        Iterator<Map.Entry<Long, Double>> iteratorWireless = WirelessMap.entrySet().iterator();
        while (iteratorWireless.hasNext()) {
            Map.Entry<Long, Double> map = iteratorWireless.next();
            // TODO 执行写入tair操作
            LOG.info(">>> 无线端：" + map.getKey() + "\t-->\t" + map.getValue());
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
