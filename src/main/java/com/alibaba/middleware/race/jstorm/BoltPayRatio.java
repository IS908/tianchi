package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceConstant;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.TableItemFactory;
import com.google.common.util.concurrent.AtomicDouble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by kevin on 16-6-26.
 */
public class BoltPayRatio implements IRichBolt {
    private static final long serialVersionUID = -1910650485341329191L;
    private static Logger LOG = LoggerFactory.getLogger(BoltPayRatio.class);

    private Map<Long, AtomicDouble> wirelessMap = new HashMap<>();
    private Map<Long, AtomicDouble> pcMap = new HashMap<>();

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    }

    @Override
    public void execute(Tuple tuple) {
        String streamId = tuple.getSourceStreamId();
        if (streamId.equals(RaceConstant.STREAM_STOP)) {
            LOG.info("<<< stop signal");
            // 累加
            List<Pair> pcList = new ArrayList<>();
            for (Map.Entry<Long, AtomicDouble> entry : pcMap.entrySet()) {
                pcList.add(new Pair(entry.getKey(), entry.getValue().doubleValue()));
            }
            Collections.sort(pcList);

            Map<Long, Double> pcTotalMap = new HashMap<>();
            for (int i = 0; i < pcList.size(); i++) {
                if (i > 0) {
                    pcList.get(i).price += pcList.get(i - 1).price;
                }
                pcTotalMap.put(pcList.get(i).timestamp, pcList.get(i).price);
            }

            List<Pair> wirelessList = new ArrayList<>();
            for (Map.Entry<Long, AtomicDouble> entry : wirelessMap.entrySet()) {
                wirelessList.add(new Pair(entry.getKey(), entry.getValue().doubleValue()));
            }
            Collections.sort(wirelessList);
            for (int i = 1; i < wirelessList.size(); i++) {
                wirelessList.get(i).price += wirelessList.get(i - 1).price;
            }

            for (Pair wireless : wirelessList) {
                Double pcPrice = pcTotalMap.get(wireless.timestamp);
                if (pcPrice != null) {
                    String key = RaceConfig.prex_ratio + wireless.timestamp;
                    double value = TableItemFactory.round(wireless.price / pcPrice, 2);
                    TairOperatorImpl.getInstance().write(key, value);
//                    LOG.info("### {}:{}", key, value);
                }
            }
        } else if (streamId.equals(RaceConstant.STREAM_PAY_PLATFORM)) {
            //            long orderID = tuple.getLong(0);
            short platform = tuple.getShort(1);
            long timestamp = tuple.getLong(2);
            double price = tuple.getDouble(3);
            if (platform == 0) { // PC
                AtomicDouble oldValue = pcMap.get(timestamp);
                if (oldValue == null) {
                    oldValue = new AtomicDouble(price);
                    pcMap.put(timestamp, oldValue);
                } else {
                    oldValue.addAndGet(price);
                }
            } else { // 无线
                AtomicDouble oldValue = wirelessMap.get(timestamp);
                if (oldValue == null) {
                    wirelessMap.put(timestamp, new AtomicDouble(price));
                } else {
                    oldValue.addAndGet(price);
                }
            }
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    private class Pair implements Comparable<Pair> {
        public long timestamp;
        public double price;

        public Pair(long timestamp, double price) {
            this.timestamp = timestamp;
            this.price = price;
        }

        @Override
        public int compareTo(Pair o) {
            if (timestamp < o.timestamp) {
                return -1;
            } else if (timestamp > o.timestamp) {
                return 1;
            }
            return 0;
        }
    }
}
