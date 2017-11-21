package com.youzan.nsq.client.entity;

import static com.youzan.nsq.client.entity.ConsumeMessageFilter.EXACT_MATCH_FILTER;

/**
 * Created by lin on 17/11/20.
 */
public enum ConsumeMessageFilterMode {
    EXACT_MATCH(EXACT_MATCH_FILTER);
//    REGEXP_MATCH,
//    GLOB_MATCH

    private ConsumeMessageFilter currentMessageFilter;

    ConsumeMessageFilterMode(ConsumeMessageFilter filter){
        currentMessageFilter = filter;
    }

    public ConsumeMessageFilter getFilter() {
        return this.currentMessageFilter;
    }
}
