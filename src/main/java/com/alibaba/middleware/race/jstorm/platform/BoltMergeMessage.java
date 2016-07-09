package com.alibaba.middleware.race.jstorm.platform;

import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConstant;
import com.alibaba.middleware.race.model.OrderInfo;
import com.alibaba.middleware.race.model.PayInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by kevin on 16-7-3.
 */
public class BoltMergeMessage implements IRichBolt {
    private static final long serialVersionUID = 7124715562791604109L;
    private static Logger LOG = LoggerFactory.getLogger(BoltMergeMessage.class);
    OutputCollector collector;
    private HashMap<Long, OrderInfo> orderMap = new HashMap<>();
    private HashMap<Long, PayInfo> payMap = new HashMap<>();

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        if (tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)) {
            return;
        }

        String streamId = tuple.getSourceStreamId();
        if (streamId.equals(RaceConstant.STREAM_ORDER_PLATFORM)) {
            Long orderId = tuple.getLongByField(RaceConstant.orderId);
            String platform = tuple.getStringByField(RaceConstant.orderPlatform);
            Double price = tuple.getDoubleByField(RaceConstant.orderPrice);
            // 配对操作
            PayInfo payInfo = payMap.get(orderId);
            if (payInfo == null) {
                orderMap.put(orderId, new OrderInfo(platform, price));
                return;
            }
            // 根据订单信息的平台划分，区分发射流
            switch (platform) {
                case RaceConstant.platformTB:
                    this.collector.emit(RaceConstant.STREAM_PLATFORM_TB,
                            new Values(payInfo.getTimestamp(), payInfo.getPrice()));
                    break;
                case RaceConstant.platformTM:
                    this.collector.emit(RaceConstant.STREAM_PLATFORM_TM,
                            new Values(payInfo.getTimestamp(), payInfo.getPrice()));
                    break;
                default:
                    break;
            }
            // 移除操作
            double res = price - payInfo.getPrice();
            if (res > 0) {
                orderMap.put(orderId, new OrderInfo(platform, res));
            } else {
                orderMap.remove(orderId);
            }
            payMap.remove(orderId);
        } else if (streamId.equals(RaceConstant.STREAM2MERGE)) {
            Long orderId = tuple.getLongByField(RaceConstant.payId);
            long timestamp = tuple.getLongByField(RaceConstant.payTime);
            double price = tuple.getDoubleByField(RaceConstant.payAmount);

            // 配对操作
            OrderInfo orderInfo = orderMap.get(orderId);
            if (orderInfo == null) {
                payMap.put(orderId, new PayInfo(timestamp, price));
                return;
            }
            // 根据订单信息的平台划分，区分发射流
            switch (orderInfo.getPaltform()) {
                case RaceConstant.platformTB:
                    this.collector.emit(RaceConstant.STREAM_PLATFORM_TB,
                            new Values(timestamp, price));
                    break;
                case RaceConstant.platformTM:
                    this.collector.emit(RaceConstant.STREAM_PLATFORM_TM,
                            new Values(timestamp, price));
                    break;
                default:
                    break;
            }
            // 移除操作
            double res = orderInfo.getPrice() - price;
            if (res > 0) {
                orderMap.put(orderId, new OrderInfo(orderInfo.getPaltform(), res));
            } else {
                orderMap.remove(orderId);
            }
            payMap.remove(orderId);
        }

    }

    @Override
    public void cleanup() {
        // TODO 该类的流程完成后的清理操作，supervisor会执行 kill -9 的操作，因此并不能保证会执行
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(RaceConstant.STREAM_PLATFORM_TB,
                new Fields(RaceConstant.payTime, RaceConstant.payAmount));
        declarer.declareStream(RaceConstant.STREAM_PLATFORM_TM,
                new Fields(RaceConstant.payTime, RaceConstant.payAmount));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
