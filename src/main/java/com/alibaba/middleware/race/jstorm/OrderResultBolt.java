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
 * Created by kevin on 16-7-3.
 */
public class OrderResultBolt implements IRichBolt {
    private static final long serialVersionUID = -837272193660107470L;
    private static Logger LOG = LoggerFactory.getLogger(OrderResultBolt.class);

    private ConcurrentHashMap<Long, Double> tbMap = null;
    private ConcurrentHashMap<Long, Double> tmMap = null;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.tbMap = new ConcurrentHashMap<>();
        this.tmMap = new ConcurrentHashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        Object obj = tuple.getValue(0);
        SumMessage message = (SumMessage) obj;
        if (message.getPlatform() == 0) {
            this.opRresult(RaceConfig.prex_taobao, message, tbMap);
        } else {
            this.opRresult(RaceConfig.prex_tmall, message, tmMap);
        }
    }

    private void opRresult(String key, SumMessage message, ConcurrentHashMap<Long, Double> map) {
        Long timestamp = message.getTimestamp();
        Double account = map.get(timestamp);
        if (account == null) {
            account = 0.0d;
            Double res = map.get(timestamp - 60L);
            TairOperatorImpl.getInstance().write(key + timestamp, res);
            map.remove(timestamp - 180L);
        }
        account += message.getTotal();
        map.put(timestamp, account);
    }

    @Override
    public void cleanup() {
        // 关闭前将最后的结果写入 tair 中
        Iterator<Map.Entry<Long, Double>> iteratorTB = tbMap.entrySet().iterator();
        while (iteratorTB.hasNext()) {
            Map.Entry<Long, Double> map = iteratorTB.next();
            TairOperatorImpl.getInstance().write(RaceConfig.prex_taobao + map.getKey(), map.getValue());
        }

        Iterator<Map.Entry<Long, Double>> iteratorTM = tmMap.entrySet().iterator();
        while (iteratorTM.hasNext()) {
            Map.Entry<Long, Double> map = iteratorTM.next();
            TairOperatorImpl.getInstance().write(RaceConfig.prex_tmall + map.getKey(), map.getValue());
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