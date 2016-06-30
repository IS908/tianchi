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
import com.alibaba.middleware.race.model.PaymentMessage;
import com.esotericsoftware.minlog.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

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
		/*
		 * if (listPC.size() > 0) {
		 * collector.emit(RaceConfig.Field_Platform_PC, new Values(listPC));
		 * }
		 * if (listWireless.size() > 0) {
		 * collector.emit(RaceConfig.Field_Platform_Wireless, new Values(listWireless));
		 * }
		 */
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(RaceConfig.Field_Platform_PC, new Fields("pcc"));
		declarer.declareStream(RaceConfig.Field_Platform_Wireless, new Fields("wirelesss"));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		LOG.debug(">>>>>> execute method prepare()");
		this.collector = collector;
	}

	@Override
	public void cleanup() {
		LOG.debug(">>>>>> execute method cleanup()");

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Log.debug(">>>>>> execute method getComponentConfiguration()");
		Config conf = new Config();
		return conf;
	}
}
