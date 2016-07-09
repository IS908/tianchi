package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import clojure.lang.Atom;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceConstant;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.TableItemFactory;
import com.google.common.util.concurrent.AtomicDouble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.Pipe;
import java.util.*;

/**
 * Created by kevin on 16-6-26.
 */
public class BoltPayRatio implements IRichBolt {
    private static final long serialVersionUID = -1910650485341329191L;
    private static Logger LOG = LoggerFactory.getLogger(BoltPayRatio.class);

    private Map<Long, AtomicDouble> wirelessMap = new HashMap<>();
    private Map<Long, AtomicDouble> pcMap = new HashMap<>();
    private long pcMaxTimestamp = 0L;
    private long wirelessMaxTimestamp = 0L;
    private Set<Long> alterTimeSet = new HashSet<>();


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    }

    @Override
    public void execute(Tuple tuple) {
        String componentId = tuple.getSourceComponent();
        String streamId = tuple.getSourceStreamId();

        if (Constants.SYSTEM_COMPONENT_ID.equals(componentId)
                && Constants.SYSTEM_TICK_STREAM_ID.equals(streamId)){
            // tick stream singal
            // 开始写数据
            write2Tair();
        } else if (RaceConstant.STREAM_STOP.equals(streamId)) {
            // 停止信号
            write2Tair();
            LOG.info("stop signal");
        } else if (streamId.equals(RaceConstant.STREAM_PAY_PLATFORM)) {
            short platform = tuple.getShortByField(RaceConstant.payPlatform);
            long timestamp = tuple.getLongByField(RaceConstant.payTime);
            double price = tuple.getDoubleByField(RaceConstant.payAmount);
            if (platform == 0) { // PC
                if (timestamp < pcMaxTimestamp) {
                    countRepair(pcMap, price, timestamp, pcMaxTimestamp);
                    // 该tuple的时间小于最大时间，出现了乱序
                } else if(timestamp == pcMaxTimestamp) {
                    // tuple的时间是正在处理的时间，该tuple属于当前的时间
                    AtomicDouble oldValue = pcMap.get(timestamp);
                    if (oldValue == null) {
                        oldValue = new AtomicDouble(price);
                    } else {
                        oldValue.addAndGet(price);
                    }
                    pcMap.put(timestamp, oldValue);
                    alterTimeSet.add(timestamp);
                } else {// timestamp > maxPcTimestamp
                    // tuple的时间 大于最大时间，新的一分钟的数据出现了
                    AtomicDouble total = pcMap.get(pcMaxTimestamp);
                    if (total != null) {
                        pcMap.put(timestamp, new AtomicDouble(price + total.doubleValue()));
                    } else {
                        pcMap.put(timestamp, new AtomicDouble(price));
                    }
                    pcMaxTimestamp = timestamp;

//                    lastTimestamp = maxTimestamp;
//                    maxTimestamp = timestamp;
//                    AtomicDouble lastPrice = pcMap.get(lastTimestamp);
//                    if (lastPrice != null) {
//                        pcMap.put(timestamp, new AtomicDouble(price + lastPrice.doubleValue()));
//                    } else {
//                        pcMap.put(timestamp, new AtomicDouble(price));
//                    }
                }
            } else { // 无线
                if (timestamp < wirelessMaxTimestamp) {
                    countRepair(wirelessMap, price, timestamp, wirelessMaxTimestamp);
                    // 该tuple的时间小于最大时间
                } else if(timestamp == wirelessMaxTimestamp) {
                    // tuple的时间是正在处理的时间，该tuple属于当前的时间
                    AtomicDouble oldValue = wirelessMap.get(timestamp);
                    if (oldValue == null) {
                        oldValue = new AtomicDouble(price);
                    } else {
                        oldValue.addAndGet(price);
                    }
                    wirelessMap.put(timestamp, oldValue);
                    alterTimeSet.add(timestamp);
                } else {
                    // tuple的时间 大于最大时间，新的一分钟的数据出现了
                    AtomicDouble total = wirelessMap.get(wirelessMaxTimestamp);
                    if (total != null) {
                        wirelessMap.put(timestamp, new AtomicDouble(price + total.doubleValue()));
                    } else {
                        wirelessMap.put(timestamp, new AtomicDouble(price));
                    }
                    wirelessMaxTimestamp = timestamp;

//                    lastTimestamp = maxTimestamp;
//                    maxTimestamp = timestamp;
//                    AtomicDouble lastPrice = wirelessMap.get(lastTimestamp);
//                    if (lastPrice != null) {
//                        wirelessMap.put(timestamp, new AtomicDouble(price + lastPrice.doubleValue()));
//                    } else {
//                        wirelessMap.put(timestamp, new AtomicDouble(price));
//                    }
                }
            }
        }
    }

    private void countRepair(Map<Long, AtomicDouble> map, double price, long cur_timestamp, long max_timestamp) {
        AtomicDouble curTotal = map.get(cur_timestamp);
        if (curTotal == null) {
            curTotal = new AtomicDouble(0.0);
        }
        curTotal.addAndGet(price);
        map.put(cur_timestamp, curTotal);

        AtomicDouble maxTotal = map.get(max_timestamp);
        if (maxTotal == null) {
            maxTotal = new AtomicDouble(0.0);
        }
        maxTotal.addAndGet(price);
        map.put(max_timestamp, maxTotal);
    }

    private void write2Tair() {
        if (!alterTimeSet.isEmpty()) {
            for (Long time : alterTimeSet) {
                AtomicDouble wirelessPrice = wirelessMap.get(time);
                AtomicDouble pcPrice = pcMap.get(time);
                if (wirelessPrice != null && pcPrice != null) {
                    double ratio = TableItemFactory.round(wirelessPrice.doubleValue() / pcPrice.doubleValue(), 2);
                    TairOperatorImpl.getInstance().write(RaceConfig.prex_ratio + time, ratio);
                }
            }
            alterTimeSet.clear();
        }
    }

    @Override
    public void cleanup() {
        write2Tair();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
