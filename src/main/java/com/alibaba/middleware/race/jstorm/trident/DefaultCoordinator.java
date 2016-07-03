package com.alibaba.middleware.race.jstorm.trident;

import com.alibaba.middleware.race.model.PaymentMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.spout.ITridentSpout;

import java.io.Serializable;

/**
 * Created by kevin on 16-6-29.
 */
public class DefaultCoordinator implements ITridentSpout.BatchCoordinator<PaymentMessage>, Serializable {
    private static final long serialVersionUID = 2285859276913939707L;

    private static final Logger LOG = LoggerFactory.getLogger(DefaultCoordinator.class);

    @Override
    public PaymentMessage initializeTransaction(long txid, PaymentMessage prevMetadata, PaymentMessage currMetadata) {
        LOG.info(">>>>>> initializeTransaction [" + txid + "]");
        return null;
    }

    @Override
    public void success(long txid) {
        LOG.info(">>>>>> success transaction [" + txid + "]");
    }

    @Override
    public boolean isReady(long txid) {

        return true;
    }

    @Override
    public void close() {

    }
}
