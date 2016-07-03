package com.alibaba.middleware.race.jstorm.trident;

import com.alibaba.middleware.race.model.PaymentMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Created by kevin on 16-6-29.
 */
public class Split extends BaseFunction {
    private static final long serialVersionUID = -7656070572246313137L;

    private static final Logger LOG = LoggerFactory.getLogger(Split.class);

    public void execute(TridentTuple tuple, TridentCollector collector) {
        PaymentMessage message = (PaymentMessage)tuple.getValue(0);
        LOG.info(message.toString());
    }
}
