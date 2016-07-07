package com.alibaba.middleware.race.jstorm;

import java.util.Map;

import com.alibaba.middleware.race.RaceConstant;
import com.sun.org.apache.xpath.internal.operations.Or;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 将拉取到的数据分流到各个处理下游
 */
public class BoltSplitStream implements IRichBolt {
	private static Logger LOG = LoggerFactory.getLogger(BoltSplitStream.class);

	private OutputCollector collector;

	@Override
	public void execute(Tuple tuple) {
		Object obj = tuple.getValue(0);
		if (obj instanceof OrderMessage) {
			OrderMessage message = (OrderMessage) obj;
			// 按平台（天猫/淘宝）划分支付消息数据流
//			if (message.getSalerId().contains("tb_saler")) {
//				// 淘宝平台订单数据
//				collector.emit(RaceConstant.STREAM_ORDER_PLATFORM,
//						new Values(message.getOrderId(), "tb", message.getTotalPrice()));
//			} else if (message.getSalerId().contains("tm_saler")) {
//				// 天猫平台订单数据
//				collector.emit(RaceConstant.STREAM_ORDER_PLATFORM,
//						new Values(message.getOrderId(), "tm", message.getTotalPrice()));
//			}
//			LOG.info("### orderMessage: {}", message);
		} else if (obj instanceof PaymentMessage) {
			PaymentMessage message = (PaymentMessage) obj;
			collector.emit(RaceConstant.STREAM_PAY_PLATFORM,
					new Values(message.getOrderId(), message.getPayPlatform(), (message.getCreateTime()/(60 * 1000)) * 60, message.getPayAmount()));
		} else {
			LOG.info("### got the end signal!!!");
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// 支付：订单ID，分钟时间戳，支付金额，支付平台
		declarer.declareStream(RaceConstant.STREAM_PAY_PLATFORM,
				new Fields(RaceConstant.payId, RaceConstant.payPlatform,
						RaceConstant.payTime, RaceConstant.payAmount));

		// 订单：订单ID，平台，价格
//		declarer.declareStream(RaceConstant.STREAM_ORDER_PLATFORM,
//				new Fields(RaceConstant.orderId,
//						RaceConstant.orderPlatform, RaceConstant.orderPrice));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void cleanup() {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		return conf;
	}
}
