package com.alibaba.middleware.race.jstorm.platform;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceConstant;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.google.common.util.concurrent.AtomicDouble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by kevin on 16-7-8.
 */
public class BoltTBCount implements IRichBolt {
    private static final long serialVersionUID = -8531647739679708927L;
    private static Logger LOG = LoggerFactory.getLogger(BoltTBCount.class);

    private Map<Long, AtomicDouble> tbMap = new HashMap<>();
    private long cur_timestamp = 0L;
    private boolean flag = false;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple tuple) {
        if (flag
                && tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)) {
            // 系统计时信号，执行写tair操作
            write3Tair();
            flag = false;
        } else if (tuple.getSourceComponent().equals(RaceConstant.ID_PAIR)
                && tuple.getSourceStreamId().equals(RaceConstant.STREAM_STOP)) {
            // 结束信号，执行写tair操作
            write3Tair();
        } else if (tuple.getSourceComponent().equals(RaceConstant.ID_PAIR)
                && tuple.getSourceStreamId().equals(RaceConstant.STREAM_PLATFORM_TB)) {
            // 正常处理逻辑
            long timestamp = tuple.getLongByField(RaceConstant.payTime);
            double price = tuple.getDoubleByField(RaceConstant.payAmount);
            cur_timestamp = Math.max(timestamp, cur_timestamp);
            AtomicDouble total = tbMap.get(timestamp);
            if (total == null) {
                total = new AtomicDouble(0.0);
            }
            total.addAndGet(price);
            tbMap.put(timestamp, total);
            flag = true;
        }
    }

    private void write3Tair() {
        long before = cur_timestamp - 60L;
        long after = cur_timestamp + 60L;
        for (long timestamp = before; timestamp <= after; timestamp += 60) {
            AtomicDouble result = tbMap.get(timestamp);
            if (result != null) {
                TairOperatorImpl.getInstance().write(
                        RaceConfig.prex_taobao + timestamp, result.doubleValue());
//                LOG.info(">>> {}:{}", RaceConfig.prex_taobao + timestamp, result.doubleValue());
            }
        }
    }

    @Override
    public void cleanup() {
        write3Tair();

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 15);
        return conf;
    }
}
