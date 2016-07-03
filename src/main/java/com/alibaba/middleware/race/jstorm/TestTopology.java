/*
 * Copyright (C) 2016 Baidu, Inc. All Rights Reserved.
 */
package com.alibaba.middleware.race.jstorm;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;

import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Created by v_zhengkun on 2016/7/3.
 */
public class TestTopology {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestTopology.class);

    public static void main(String[] args) {
        Map config = new HashMap();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(RaceConfig.JstormTopologyName, config, buildTopology());
    }

    private static StormTopology buildTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("rocketMqSpout", new RocketMqSpout(), 1);
        builder.setBolt("testBolt", new TestBolt()).fieldsGrouping("rocketMqSpout", new Fields("message"));
        return builder.createTopology();
    }
}
