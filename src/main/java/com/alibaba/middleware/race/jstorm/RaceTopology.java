package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
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
        Config conf = new Config();
        try {
            StormSubmitter.submitTopology(RaceConfig.JstormTopologyName, conf, builtTopology());
        } catch (AlreadyAliveException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
        }
    }

    // TODO 正式逻辑在这里组织
    private static StormTopology builtTopology() {
        /*
        * soup -->
        * bolt -->
        * bolt -->
        * ...
        * */
        return null;
    }

    // 测试workcount示例代码
    /*public static void main(String[] args) throws Exception {

        Config conf = new Config();
        int spout_Parallelism_hint = 1;
        int split_Parallelism_hint = 2;
        int count_Parallelism_hint = 2;
        int result_Parallelism_hint = 1;

        TopologyBuilder builder = new TopologyBuilder();

        LOG.info("======>>>>>>获取数据源开始");
        builder.setSpout(RaceConfig.SPOUT_ID, new RaceSentenceSpout(), spout_Parallelism_hint);

        LOG.info("======>>>>>>切分词操作开始");
        builder.setBolt(RaceConfig.BOLT_SPLIT_ID, new SplitSentence(), split_Parallelism_hint)
                //.setNumTasks(4)
                .shuffleGrouping(RaceConfig.SPOUT_ID);

        LOG.info("======>>>>>>词计数操作开始");
        builder.setBolt(RaceConfig.BOLT_COUNT_ID, new WordCount(), count_Parallelism_hint)
                .fieldsGrouping(RaceConfig.BOLT_SPLIT_ID, new Fields(RaceConfig.BOLT_FILED_ID));

        LOG.info("======>>>>>>打印统计结果操作开始");
        builder.setBolt(RaceConfig.BOLT_RESULT_ID, new CountResult(), result_Parallelism_hint)
                .globalGrouping(RaceConfig.BOLT_COUNT_ID);

        String topologyName = RaceConfig.JstormTopologyName;

        if (args.length == 0) {
            // 本地debug的配置
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, conf, builder.createTopology());
            Utils.sleep(20000);
            cluster.killTopology(topologyName);
            cluster.shutdown();
        } else {
            // 提交到作业时的配置
            // TODO 打包修改pom包 jstorm-core 为 provided
            // jstorm jar tianchi-1.0-SNAPSHOT.jar com.alibaba.middleware.race.jstorm.RaceTopology xxx
            try {
                StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
            } catch (Exception e) {
                LOG.error(e.getLocalizedMessage());
                e.printStackTrace();
            }
        }
    }*/
}