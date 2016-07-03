package com.alibaba.middleware.race.Tair;

import com.alibaba.middleware.race.RaceConfig;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;

/**
 * 读写tair所需要的集群信息，如masterConfigServer/slaveConfigServer地址/
 * group 、namespace我们都会在正式提交代码前告知选手
 */
public class TairOperatorImpl {

    private DefaultTairManager tairManager;
    private int namespace;

    public TairOperatorImpl(String masterConfigServer, String slaveConfigServer, String groupName, int namespace) {
        tairManager = new DefaultTairManager();
        List<String> configServerList = Arrays.asList(masterConfigServer, slaveConfigServer);
        tairManager.setConfigServerList(configServerList);
        tairManager.setGroupName(groupName);
        tairManager.init();

        this.namespace = namespace;
    }

    public boolean write(Serializable key, Serializable value) {
        ResultCode resultCode = tairManager.put(namespace, key, value);
        return resultCode.isSuccess();
    }

    public Object get(Serializable key) {
        Result<DataEntry> result = tairManager.get(namespace, key);
        if (result.isSuccess() && result.getValue() != null) {
            return result.getValue().getValue();
        }
        return null;
    }

    public boolean remove(Serializable key) {
        ResultCode resultCode = tairManager.delete(namespace, key);
        return resultCode.isSuccess();
    }

    public void close() {
        tairManager.close();
    }

    //天猫的分钟交易额写入tair
    public static void main(String[] args) throws Exception {
        TairOperatorImpl tairOperator =
                new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                                            RaceConfig.TairGroup, RaceConfig.TairNamespace);
        //假设这是付款时间
        Long millisTime = System.currentTimeMillis();
        //由于整分时间戳是10位数，所以需要转换成整分时间戳
        Long minuteTime = (millisTime / 1000 / 60) * 60;
        //假设这一分钟的交易额是100;
        Double money = 100.0;
        //写入tair
        tairOperator.write(RaceConfig.prex_tmall + minuteTime, money);
    }


    private static class InstanceHolder {
        static final TairOperatorImpl tairOperator =
                new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                                            RaceConfig.TairGroup, RaceConfig.TairNamespace);
    }

    public static TairOperatorImpl getInstance() {
        return InstanceHolder.tairOperator;
    }
}
