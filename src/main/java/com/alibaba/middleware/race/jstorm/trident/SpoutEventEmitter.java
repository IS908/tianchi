package com.alibaba.middleware.race.jstorm.trident;

import com.alibaba.middleware.race.model.PaymentMessage;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout;
import storm.trident.topology.TransactionAttempt;

import java.io.Serializable;

/**
 * Created by kevin on 16-6-29.
 */
public class SpoutEventEmitter implements ITridentSpout.Emitter<PaymentMessage>, Serializable {
    private static final long serialVersionUID = -5833788542680457946L;

    @Override
    public void emitBatch(TransactionAttempt tx, PaymentMessage coordinatorMeta, TridentCollector collector) {

    }

    @Override
    public void success(TransactionAttempt tx) {

    }

    @Override
    public void close() {

    }
}
