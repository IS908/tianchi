package com.alibaba.middleware.race.jstorm;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.model.PaymentMessage;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitStreamBolt implements IRichBolt {
	private static Logger LOG = LoggerFactory.getLogger(SplitStreamBolt.class);

	OutputCollector collector;

	@Override
	public void execute(Tuple tuple) {
		Object obj = tuple.getValue(0);
		PaymentMessage message = (PaymentMessage) obj;
		// 按平台划分数据流
		if (message.getPayPlatform() == 0) {// PC 端
			collector.emit(RaceConfig.Field_Platform_PC, new Values(message));
		} else {// 无线端
			collector.emit(RaceConfig.Field_Platform_Wireless, new Values(message));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(RaceConfig.Field_Platform_PC, new Fields("pcc"));
		declarer.declareStream(RaceConfig.Field_Platform_Wireless, new Fields("wirelesss"));
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
