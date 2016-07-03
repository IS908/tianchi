package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
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
        String topologyName = RaceConfig.JstormTopologyName;


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, builtTopology().createTopology());
//        本地调试设定运行时间
//        Utils.sleep(30000);
//        cluster.killTopology(topologyName);
//        cluster.shutdown();

//        TODO 打包上传需注释上面 LocalCluster 部分，开启下面部分；同时 pom 包要开启 jstorm 的 provided
        /*try {
            StormSubmitter.submitTopology(RaceConfig.JstormTopologyName, conf, builtTopology().createTopology());
        } catch (AlreadyAliveException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
        }*/
    }

    // TODO 正式逻辑在这里组织
    private static TopologyBuilder builtTopology() {
        int spout_Parallelism_hint = 1;
        int split_Parallelism_hint = 1;
        int count_Parallelism_hint = 2;
        int result_Parallelism_hint = 1;

        // 获取订单数据
        RaceOrderMessageSpout orderSource = new RaceOrderMessageSpout();


        // 获取支付数据
        RacePaymentMessageSpout paySource = new RacePaymentMessageSpout();
        SplitStreamBolt splitBolt = new SplitStreamBolt();

        TopologyBuilder builder = new TopologyBuilder();

        //  从rocketMQ中拉取数据
        /*builder.setSpout(RaceConfig.ID_ORDER_SOURCE, orderSource, spout_Parallelism_hint);*/

        builder.setSpout(RaceConfig.ID_PAY_SOURCE, paySource, spout_Parallelism_hint);

        builder.setBolt(RaceConfig.ID_SPLIT_PLATFORM, splitBolt, split_Parallelism_hint)
                .fieldsGrouping(RaceConfig.ID_PAY_SOURCE, new Fields(RaceConfig.FIELD_PAY_DATA));

        builder.setBolt(RaceConfig.ID_PC_TIME_STAMP, new PayCountBolt(), count_Parallelism_hint)
                .fieldsGrouping(RaceConfig.ID_SPLIT_PLATFORM, RaceConfig.FIELD_PLATFORM_PC, new Fields(RaceConfig.FIELD_PAY_PC));

        builder.setBolt(RaceConfig.ID_WIRELESS_TIME_STAMP, new PayCountBolt(), count_Parallelism_hint)
                .fieldsGrouping(RaceConfig.ID_SPLIT_PLATFORM, RaceConfig.FIELD_PLATFORM_WIRELESS, new Fields(RaceConfig.FIELD_PAY_WIRELESS));

        builder.setBolt(RaceConfig.ID_PAY_RATIO, new CountResultBolt(), result_Parallelism_hint)
                .globalGrouping(RaceConfig.ID_PC_TIME_STAMP)
                .globalGrouping(RaceConfig.ID_WIRELESS_TIME_STAMP);
        return builder;
    }
}