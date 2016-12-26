package com.youzan.nsq.client.core.lookup;

import com.youzan.nsq.client.configs.TopicRuleCategory;
import com.youzan.nsq.client.core.LookupAddressUpdate;
import com.youzan.nsq.client.entity.IPartitionsSelector;
import com.youzan.nsq.client.entity.Partitions;
import com.youzan.nsq.client.entity.Role;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.entity.lookup.NSQLookupdAddress;
import com.youzan.nsq.client.exception.NSQLookupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
public class LookupServiceImpl implements LookupService {

    private static final Logger logger = LoggerFactory.getLogger(LookupServiceImpl.class);
    private static final long serialVersionUID = 1773482379917817275L;
    private final Role role;

    /**
     * Load-Balancing Strategy: round-robin
     */
    private volatile int offset = 0;
    private boolean started = false;
    private boolean closing = false;
    private volatile long lastConnecting = 0L;

    /**
     * @param role  role for producer and consumer
     */
    public LookupServiceImpl(Role role) {
        this.role = role;
    }

    @Override
    public IPartitionsSelector lookup(Topic topic, boolean localLookupd) throws NSQLookupException {
        TopicRuleCategory category = TopicRuleCategory.getInstance(this.role);
        switch (this.role) {
            case Consumer: {
                return lookup(topic, false, category, localLookupd);
            }
            case Producer: {
                return lookup(topic, true, category, localLookupd);
            }
            default: {
                throw new UnsupportedOperationException("Unknown options. Writable or Readable?");
            }
        }
    }

    @Override
    public IPartitionsSelector lookup(final Topic topic, boolean writable, final TopicRuleCategory category, boolean localLookupd) throws NSQLookupException{
        if (null == topic || null == topic.getTopicText() || topic.getTopicText().isEmpty()) {
            throw new NSQLookupException("Your input topic is blank!");
        }
        /*
         * It is unnecessary to use Atomic/Lock for the variable
         */
        NSQLookupdAddress aLookupdAddr = LookupAddressUpdate.getInstance().getLookup(topic, category, localLookupd);
        IPartitionsSelector ps = null;
        if(null != aLookupdAddr)
            ps = aLookupdAddr.lookup(topic, writable);
        return ps;
    }

    @Override
    public void close() {
        closing = true;
    }
}