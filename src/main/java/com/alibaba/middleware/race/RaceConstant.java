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


    // jstorm的拓扑流程中的个组件的ID统一在此处设置
    // ComponentID
    public static final String ID_SPOUT_SOURCE = "spout_source";//

    public static final String ID_SPLIT_PLATFORM = "split_platform";//

    public static final String ID_ORDER_TB = "order_tb";//
    public static final String ID_ORDER_TM = "order_tm";//

    public static final String ID_PAY_PC = "payment_pc";//
    public static final String ID_PAY_WIRELESS = "payment_wireless";//

    public static final String ID_PAY_RATIO = "pay_ratio";//
    public static final String ID_ORDER_SUM = "order_sum";//

    // StreamID
    public static final String STREAM_ORDER_PLATFORM = "stream_order_platform";
    public static final String STREAM_PLATFORM_TB = "stream_platform_tb";
    public static final String STREAM_PLATFORM_TM = "stream_platform_tm";
    public static final String STREAM_PAY_PLATFORM = "stream_pay_platform";
    public static final String STREAM_PLATFORM_PC = "stream_platform_pc";
    public static final String STREAM_PLATFORM_WIRELESS = "stream_platform_wireless";

    // FieldName
    public static final String FIELD_SOURCE_DATA = "message";

    public static final String FIELD_ORDER_TB = "field_order_tb";
    public static final String FIELD_ORDER_TM = "field_order_tm";

    public static final String FIELD_PAY_PC = "field_pc";
    public static final String FIELD_PAY_WIRELESS = "field_wireless";

    public static final String FIELD_PAY_SUM = "field_pay_sum";
    public static final String FIELD_ORDER_SUM = "field_order_sum";
}
