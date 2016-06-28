package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {

    // 这些是写tair key的前缀
    // 淘宝每分钟的交易金额的key更新为platformTaobao_TeamCode_整分时间戳,
    // 天猫每分钟的交易金额的key更新为platformTmall_TeamCode_整分时间戳,
    // 每整分时刻无线和PC端总交易金额比值的key 更新为ratio_TeamCode_整分时间戳，
    // TeamCode是每个队伍的唯一标识
    public static String prex_tmall = "platformTmall_41413few7x_";
    public static String prex_taobao = "platformTaobao_41413few7x_";
    public static String prex_ratio = "ratio_41413few7x_";


    // 这些jstorm/rocketMq/tair 的集群配置信息，这些配置信息在正式提交代码前会被公布
    public static String JstormTopologyName = "41413few7x";
    public static String MetaConsumerGroup = "41413few7x";
    public static String MqPayTopic = "MiddlewareRaceTestData_Pay";
    public static String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    public static String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";
    public static String TairConfigServer = "10.101.72.127:5198";
    public static String TairSalveConfigServer = "10.101.72.128:5198";
    public static String TairGroup = "group_tianchi";
    public static Integer TairNamespace = 45596;

    // jstorm的拓扑流程中的个组件的ID统一在此处设置
    public static final String SPOUT_ID = "spout";
    public static final String BOLT_SPLIT_ID = "split";
    public static final String BOLT_COUNT_ID = "count";
    public static final String BOLT_RESULT_ID = "result";

    public static final String SPOUT_FILED_ID = "sentence";
    public static final String BOLT_FILED_ID = "word";
}
