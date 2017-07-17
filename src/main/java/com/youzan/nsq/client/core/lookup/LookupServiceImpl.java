package com.youzan.nsq.client.core.lookup;

import com.youzan.nsq.client.configs.TopicRuleCategory;
import com.youzan.nsq.client.core.LookupAddressUpdate;
import com.youzan.nsq.client.entity.IPartitionsSelector;
import com.youzan.nsq.client.entity.Role;
import com.youzan.nsq.client.entity.lookup.NSQLookupdAddresses;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.exception.NSQLookupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
public class LookupServiceImpl implements LookupService {

    private static final Logger logger = LoggerFactory.getLogger(LookupServiceImpl.class);
    private static final long serialVersionUID = 1773482379917817275L;
    private final Role role;
    private int lookupLocalId = -1;

    /**
     * @param role  role for producer and consumer
     */
    public LookupServiceImpl(Role role, int lookupLocalId) {
        this.role = role;
        this.lookupLocalId = lookupLocalId;
    }

    @Override
    public IPartitionsSelector lookup(String topic, boolean localLookupd, boolean force) throws NSQException {
        TopicRuleCategory category = TopicRuleCategory.getInstance(this.role);
        switch (this.role) {
            case Consumer: {
                return lookup(topic, false, category, localLookupd, force);
            }
            case Producer: {
                return lookup(topic, true, category, localLookupd, force);
            }
            default: {
                throw new UnsupportedOperationException("Unknown options. Writable or Readable?");
            }
        }
    }

    @Override
    public IPartitionsSelector lookup(final String topic, boolean writable, final TopicRuleCategory category, boolean localLookupd, boolean force) throws NSQException {
        if (null == topic || topic.isEmpty()) {
            throw new NSQLookupException("Input topic is blank!");
        }
        /*
         * It is unnecessary to use Atomic/Lock for the variable
         */
        NSQLookupdAddresses aLookupdAddr = LookupAddressUpdate.getInstance().getLookup(topic, category, localLookupd, force, this.lookupLocalId);
        IPartitionsSelector ps = null;
        if(null != aLookupdAddr)
            ps = aLookupdAddr.lookup(topic, writable);
        return ps;
    }

    @Override
    public void close() {
    }
}