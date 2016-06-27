package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {

    // 这些是写tair key的前缀
    public static String prex_tmall = "platformTmall_";
    public static String prex_taobao = "platformTaobao_";
    public static String prex_ratio = "ratio_";


    // 这些jstorm/rocketMq/tair 的集群配置信息，这些配置信息在正式提交代码前会被公布
    public static String JstormTopologyName = "xxx";
    public static String MetaConsumerGroup = "xxx";
    public static String MqPayTopic = "MiddlewareRaceTestData_Pay";
    public static String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    public static String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";
    public static String TairConfigServer = "xxx";
    public static String TairSalveConfigServer = "xxx";
    public static String TairGroup = "xxx";
    public static Integer TairNamespace = 1;

    // jstorm的拓扑流程中的个组件的ID统一在此处设置
    public static final String SPOUT_ID = "spout";
    public static final String BOLT_SPLIT_ID = "split";
    public static final String BOLT_COUNT_ID = "count";
    public static final String BOLT_RESULT_ID = "result";

    public static final String SPOUT_FILED_ID = "sentence";
    public static final String BOLT_FILED_ID = "word";
}