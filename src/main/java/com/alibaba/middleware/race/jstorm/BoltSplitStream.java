package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConstant;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 将拉取到的数据分流到各个处理下游
 */
public class BoltSplitStream implements IRichBolt {
	private static final long serialVersionUID = 6264734155123954277L;
	private static Logger LOG = LoggerFactory.getLogger(BoltSplitStream.class);

	private OutputCollector collector;

	@Override
	public void execute(Tuple tuple) {
		String type = tuple.getStringByField(RaceConstant.FIELD_TYPE);
		if (type.equals(RaceConstant.stop)) {
			collector.emit(RaceConstant.STREAM_STOP, new Values("stop"));
			LOG.info("### got the end signal!!!");
			return;
		} else if (type.equals("order")) {
			OrderMessage message = (OrderMessage) tuple.getValueByField(RaceConstant.FIELD_SOURCE_DATA);
			// 按平台（天猫/淘宝）划分支付消息数据流
			if (message.getSalerId().contains("tb_saler")) {
				// 淘宝平台订单数据
				collector.emit(RaceConstant.STREAM_ORDER_PLATFORM,
						new Values(message.getOrderId(),
								RaceConstant.platformTB,
								message.getTotalPrice()));
			} else if (message.getSalerId().contains("tm_saler")) {
				// 天猫平台订单数据
				collector.emit(RaceConstant.STREAM_ORDER_PLATFORM,
						new Values(message.getOrderId(),
								RaceConstant.platformTM,
								message.getTotalPrice()));
			}
//			LOG.info("### orderMessage: {}", message);
		} else if (type.equals("pay")) {
			PaymentMessage message = (PaymentMessage) tuple.getValueByField(RaceConstant.FIELD_SOURCE_DATA);
			long timestamp = (message.getCreateTime()/(60 * 1000)) * 60;
			collector.emit(RaceConstant.STREAM_PAY_PLATFORM,
					new Values(message.getOrderId(), message.getPayPlatform(),
							timestamp, message.getPayAmount()));

			collector.emit(RaceConstant.STREAM2MERGE,
					new Values(message.getOrderId(), timestamp, message.getPayAmount()));
//			LOG.info("### paymentMessage: {}", message);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// 支付：订单ID，支付平台，分钟时间戳，支付金额
		declarer.declareStream(RaceConstant.STREAM_PAY_PLATFORM,
				new Fields(RaceConstant.payId, RaceConstant.payPlatform,
						RaceConstant.payTime, RaceConstant.payAmount));


		declarer.declareStream(RaceConstant.STREAM2MERGE,
				new Fields(RaceConstant.payId, RaceConstant.payTime, RaceConstant.payAmount));
		// 订单：订单ID，平台，价格
		declarer.declareStream(RaceConstant.STREAM_ORDER_PLATFORM,
				new Fields(RaceConstant.orderId,
						RaceConstant.orderPlatform, RaceConstant.orderPrice));

		// 发送停止消息
		declarer.declareStream(RaceConstant.STREAM_STOP, new Fields("stop"));
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
		return null;
	}
}
