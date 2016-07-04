package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.alibaba.middleware.race.RaceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 这是一个很简单的例子
 * 选手的拓扑提交到集群，我们是有超时设置的。每个选手的拓扑最多跑20分钟，一旦超过这个时间
 * 我们会将选手拓扑杀掉。
 */

/**
 * 选手拓扑入口类，我们定义必须是com.alibaba.middleware.race.jstorm.RaceTopology
 * 因为我们后台对选手的git进行下载打包，拓扑运行的入口类默认是com.alibaba.middleware.race.jstorm.RaceTopology；
 * 所以这个主类路径一定要正确
 */
public class RaceTopology {
    private static Logger LOG = LoggerFactory.getLogger(RaceTopology.class);

    public static void main(String[] args) {
        /*
         * 全局config，局部配置在每个组件的 getComponentConfiguration() 方法中定义
         * 优先级：局部 > 全局
         */
        Config conf = new Config();

//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology(RaceConfig.JstormTopologyName, conf, builtTopology().createTopology());
//        本地调试设定运行时间
//        Utils.sleep(30000);
//        cluster.killTopology(topologyName);
//        cluster.shutdown();

//        TODO 打包上传需注释上面 LocalCluster 部分，开启下面部分；同时 pom 包要开启 jstorm 的 provided
        try {
            StormSubmitter.submitTopology(RaceConfig.JstormTopologyName, conf, builtTopology().createTopology());
        } catch (AlreadyAliveException | InvalidTopologyException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
        }
    }

    // 正式逻辑在这里组织
    private static TopologyBuilder builtTopology() {
        int spout_Parallelism_hint = 1;
        int split_Parallelism_hint = 2;
        int count_Parallelism_hint = 1;
        int result_Parallelism_hint = 1;

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(RaceConfig.ID_SPOUT_SOURCE, new RocketMqSpout(), spout_Parallelism_hint);

        //  从rocketMQ中拉取数据
        builder.setBolt(RaceConfig.ID_SPLIT_PLATFORM, new SplitStreamBolt(), split_Parallelism_hint)
                .fieldsGrouping(RaceConfig.ID_SPOUT_SOURCE, new Fields(RaceConfig.FIELD_SOURCE_DATA));

        // 淘宝/天猫 订单数据分析
        builder.setBolt(RaceConfig.ID_ORDER_TB, new OrderCountTBBolt(), count_Parallelism_hint)
                .fieldsGrouping(RaceConfig.ID_SPLIT_PLATFORM, RaceConfig.STREAM_PLATFORM_TB, new Fields(RaceConfig.FIELD_ORDER_TB));

        builder.setBolt(RaceConfig.ID_ORDER_TM, new OrderCountTMBolt(), count_Parallelism_hint)
                .fieldsGrouping(RaceConfig.ID_SPLIT_PLATFORM, RaceConfig.STREAM_PLATFORM_TM, new Fields(RaceConfig.FIELD_ORDER_TM));

        builder.setBolt(RaceConfig.ID_ORDER_RATIO, new OrderResultBolt(), result_Parallelism_hint)
                .globalGrouping(RaceConfig.ID_ORDER_TB)
                .globalGrouping(RaceConfig.ID_ORDER_TM);

        // PC端/无线端 支付数据分析
        builder.setBolt(RaceConfig.ID_PAY_PC, new PayCountPCBolt(), count_Parallelism_hint)
                .fieldsGrouping(RaceConfig.ID_SPLIT_PLATFORM, RaceConfig.STREAM_PLATFORM_PC, new Fields(RaceConfig.FIELD_PAY_PC));

        builder.setBolt(RaceConfig.ID_PAY_WIRELESS, new PayCountWirelessBolt(), count_Parallelism_hint)
                .fieldsGrouping(RaceConfig.ID_SPLIT_PLATFORM, RaceConfig.STREAM_PLATFORM_WIRELESS, new Fields(RaceConfig.FIELD_PAY_WIRELESS));

        builder.setBolt(RaceConfig.ID_PAY_RATIO, new PayResultBolt(), result_Parallelism_hint)
                .globalGrouping(RaceConfig.ID_PAY_PC)
                .globalGrouping(RaceConfig.ID_PAY_WIRELESS);
        return builder;
    }
}