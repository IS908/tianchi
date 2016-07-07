package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConstant;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.google.common.util.concurrent.AtomicDouble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by kevin on 16-6-26.
 */
public class BoltPayRatio implements IRichBolt {
    private static final long serialVersionUID = -1910650485341329191L;
    private static Logger LOG = LoggerFactory.getLogger(BoltPayRatio.class);
    private OutputCollector collector;

    private HashMap<Long, AtomicDouble> wirelessMap = new HashMap<>();
    private HashMap<Long, AtomicDouble> pcMap = new HashMap<>();
    private long timestamp = 0L;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String streamId = tuple.getSourceStreamId();
        if (streamId.equals(RaceConstant.STREAM_STOP)) {
            LOG.info("### streamId {} got the end signal!");
        } else if (streamId.equals(RaceConstant.STREAM_PAY_PLATFORM)) {
            long orderID = tuple.getLong(0);
            short platform = tuple.getShort(1);
            long timestamp = tuple.getLong(2);
            double price = tuple.getDouble(3);

            if (platform == 0) {    // PC
                AtomicDouble pcPrice = pcMap.get(timestamp);
                if (pcPrice == null) {
                    AtomicDouble beforSum = pcMap.get(timestamp - 60L);
                    double befor = 0.0d;
                    if (beforSum != null) {
                        befor = beforSum.doubleValue();
                    }
                    pcPrice = new AtomicDouble(befor);
                }
                pcPrice.addAndGet(price);
                pcMap.put(timestamp, pcPrice);
            } else {    // 无线
                AtomicDouble wirelessPrice = wirelessMap.get(timestamp);
                if (wirelessPrice == null) {
                    AtomicDouble beforSum = wirelessMap.get(timestamp - 60L);
                    double befor = 0.0d;
                    if (beforSum != null) {
                        befor = beforSum.doubleValue();
                    }
                    wirelessPrice = new AtomicDouble(befor);
                }
                wirelessPrice.addAndGet(price);
                wirelessMap.put(timestamp, wirelessPrice);
            }

            if (this.timestamp == 0) {
                return;
            }
            if (this.timestamp < timestamp) {

                String res = String.format("%.2f",
                        wirelessMap.get(this.timestamp).doubleValue() / pcMap.get(this.timestamp).doubleValue());
//                TairOperatorImpl.getInstance().write(this.timestamp, res);
                LOG.info(">>> ratio : {}", res);
                this.timestamp = timestamp;
            } /*else if (this.timestamp > timestamp){
                double wireless, pc;
                while (timestamp < this.timestamp) {
                    wireless = wirelessMap.get(timestamp).doubleValue();
                    pc = pcMap.get(timestamp).doubleValue();
                    TairOperatorImpl.getInstance().write(timestamp, wireless / pc);
                    timestamp += 60L;
                }
            }*/

        }




    }

    @Override
    public void cleanup() {
        // TODO 关闭前将最后的结果写入 tair 中
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
