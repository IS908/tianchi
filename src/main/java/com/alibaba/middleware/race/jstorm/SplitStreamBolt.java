package com.alibaba.middleware.race.jstorm;

import java.util.Map;

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
public class SplitStreamBolt implements IRichBolt {
	private static Logger LOG = LoggerFactory.getLogger(SplitStreamBolt.class);

	private OutputCollector collector;

	@Override
	public void execute(Tuple tuple) {
		Object obj = tuple.getValue(0);
		if (obj instanceof OrderMessage) {
			OrderMessage message = (OrderMessage) obj;
			// 按平台（天猫/淘宝）划分支付消息数据流
			if (message.getSalerId().contains("tb_saler")) {
				// 淘宝平台订单数据
				collector.emit(RaceConfig.STREAM_ORDER_PLATFORM,
						new Values(message.getOrderId(), "tb", message.getTotalPrice()));
			} else if (message.getSalerId().contains("tm_saler")) {
				// 天猫平台订单数据
				collector.emit(RaceConfig.STREAM_ORDER_PLATFORM,
						new Values(message.getOrderId(), "tm", message.getTotalPrice()));
			}
		} else if (obj instanceof PaymentMessage) {
			PaymentMessage message = (PaymentMessage) obj;
			collector.emit(RaceConfig.STREAM_PAY_PLATFORM,
					// values[ 订单ID, 创建时间, 支付金额， 支付平台(无线/PC)]
					new Values(message.getOrderId(), (message.getCreateTime()/(60 * 1000)) * 60, message.getPayAmount(), message.getPayPlatform()));
		}
		this.collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// 支付：订单ID，分钟时间戳，支付金额，支付平台
		declarer.declareStream(RaceConfig.STREAM_PAY_PLATFORM,
				new Fields("Id", "timestamp", "payAmount", "platform"));

		// 订单：订单ID，平台，价格
		declarer.declareStream(RaceConfig.STREAM_ORDER_PLATFORM,
				new Fields("Id", "platform", "price"));
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
