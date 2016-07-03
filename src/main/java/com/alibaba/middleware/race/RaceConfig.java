package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {

    // TODO rocketMQ的nameserver地址及端口统一在此配置
    public static final String MQ_NAME_SERVER = "192.168.1.10:9876";

    // TODO jstorm的拓扑流程中的个组件的ID统一在此处设置
    // ComponentID
    public static final String id_spout_source = "id_spout_source";//

    public static final String id_order_tb = "id_order_tb";//
    public static final String id_order_tm = "id_order_tm";//

    public static final String Id_pay_pc = "PCTimestamp";//
    public static final String id_pay_wireless = "WirelessTimestamp";//
    public static final String ID_PAY_RATIO = "payRatio";//
    public static final String ID_ORDER_RATIO = "orderRatio";//

    // StreamID
    public static final String STREAM_PLATFORM_TB = "stream_platform_tb";
    public static final String STREAM_PLATFORM_TM = "stream_platform_tm";
    public static final String STREAM_PLATFORM_PC = "stream_platform_pc";
    public static final String STREAM_PLATFORM_WIRELESS = "stream_platform_wireless";

    public static final String FIELD_TB_ORDER_DATA = "tb_message";
    public static final String FIELD_TM_ORDER_DATA = "tm_message";
    public static final String ID_PAY_STREAM = "paystream";
    public static final String Stream_pay_pc = "stream_pc";
    public static final String Stream_pay_wireless = "stream_wireless";

    ///////////////////////
    public static final String ID_SPLIT_PLATFORM = "platform";//

    ///////////////////////

    // FieldName
    public static final String FIELD_SOURCE_DATA = "message";

    public static final String field_order_tb = "field_order_tb";
    public static final String field_order_tm = "field_order_tm";

    public static final String FIELD_PAY_PC = "pcfield";
    public static final String FIELD_PAY_WIRELESS = "wirelessfield";

    public static final String FIELD_PAY_DATA = "paydata";
    public static final String field_pay_sum = "field_pay_sum";
    public static final String field_order_sum = "field_order_sum";



    /* =================================================================
    * 这些是写tair key的前缀
    * 淘宝每分钟的交易金额的key更新为platformTaobao_TeamCode_整分时间戳,
    * 天猫每分钟的交易金额的key更新为platformTmall_TeamCode_整分时间戳,
    * 每整分时刻无线和PC端总交易金额比值的key 更新为ratio_TeamCode_整分时间戳，
    * TeamCode是每个队伍的唯一标识
    * */
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

}
