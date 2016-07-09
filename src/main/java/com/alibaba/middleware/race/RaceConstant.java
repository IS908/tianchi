package com.alibaba.middleware.race;

/**
 * Created by kevin on 16-7-7.
 */
public class RaceConstant {
    // Fields 各列信息
    // orderMessage 列名信息
    public static final String orderId = "id";
    public static final String orderPrice = "price";
    public static final String orderPlatform = "platform";
    // payMessage 列名信息
    public static final String payId = "id";
    public static final String payTime = "time";
    public static final String payAmount = "price";
    public static final String payPlatform = "platform";

    public static final String platformTB = "tb";
    public static final String platformTM = "tm";


    // jstorm的拓扑流程中的个组件的ID统一在此处设置
    // ComponentID
    public static final String ID_SPOUT_SOURCE = "spout_source";//

    public static final String ID_SPLIT_PLATFORM = "split_platform";//

    public static final String ID_PAIR = "pair";
    public static final String ID_ORDER_TB = "order_tb";//
    public static final String ID_ORDER_TM = "order_tm";//

    public static final String ID_PAY_RATIO = "pay_ratio";//

    // StreamID
    public static final String STREAM_STOP = "stop";
    public static final String STREAM_ORDER_PLATFORM = "order_platform";
    public static final String STREAM_PLATFORM_TB = "platform_tb";
    public static final String STREAM_PLATFORM_TM = "platform_tm";
    public static final String STREAM_PAY_PLATFORM = "pay_platform";

    // FieldName
    public static final String FIELD_TYPE = "type";
    public static final String FIELD_SOURCE_DATA = "message";
}
