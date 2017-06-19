package com.youzan.nsq.client.entity.lookup;

import com.youzan.nsq.client.configs.DCCSeedLookupdConfig;

import java.lang.ref.SoftReference;
import java.util.List;

/**
 * Created by lin on 16/12/5.
 */
public abstract class AbstractSeedLookupdConfig extends AbstractLookupdConfig {

    public abstract List<SoftReference<SeedLookupdAddress>> getSeedLookupAddress(String categorization, String topic);

    public abstract NSQLookupdAddresses punchLookupdAddress(String categorization, final String topic, boolean force);

    public static AbstractSeedLookupdConfig create(String categorization) {
        return new DCCSeedLookupdConfig(categorization);
    }
}
