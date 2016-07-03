package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 将拉取到的数据分流到各个处理下游
 */
public class SplitStreamBolt implements IRichBolt {
	private static Logger LOG = LoggerFactory.getLogger(SplitStreamBolt.class);

	OutputCollector collector;

	@Override
	public void execute(Tuple tuple) {
		Object obj = tuple.getValue(0);
		if (obj instanceof OrderMessage) {
			OrderMessage message = (OrderMessage) obj;
			// 按平台（天猫/淘宝）划分支付消息数据流
			if (message.getSalerId().contains("tb_saler")) {
				// 淘宝平台订单数据流向
				collector.emit(RaceConfig.STREAM_PLATFORM_TB, new Values(message));
			} else if (message.getSalerId().contains("tm_saler")) {
				// 天猫平台订单数据流向
				collector.emit(RaceConfig.STREAM_PLATFORM_TM, new Values(message));
			}
		} else if (obj instanceof PaymentMessage) {
			PaymentMessage message = (PaymentMessage) obj;
			// 按平台（PC/无线）划分支付消息数据流
			if (message.getPayPlatform() == 0) {
				// PC 端支付数据流向
				collector.emit(RaceConfig.STREAM_PLATFORM_PC, new Values(message));
			} else if (message.getPayPlatform() == 1){
				// 无线端支付数据流向
				collector.emit(RaceConfig.STREAM_PLATFORM_WIRELESS, new Values(message));
			}
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(RaceConfig.STREAM_PLATFORM_TB, new Fields(RaceConfig.FIELD_ORDER_TB));
		declarer.declareStream(RaceConfig.STREAM_PLATFORM_TM, new Fields(RaceConfig.FIELD_ORDER_TM));
		declarer.declareStream(RaceConfig.STREAM_PLATFORM_PC, new Fields(RaceConfig.FIELD_PAY_PC));
		declarer.declareStream(RaceConfig.STREAM_PLATFORM_WIRELESS, new Fields(RaceConfig.FIELD_PAY_WIRELESS));

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
