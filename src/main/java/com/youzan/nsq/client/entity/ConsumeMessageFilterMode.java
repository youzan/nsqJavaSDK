package com.youzan.nsq.client.entity;

import static com.youzan.nsq.client.entity.ConsumeMessageFilter.*;

/**
 * Created by lin on 17/11/20.
 */
public enum ConsumeMessageFilterMode {
    EXACT_MATCH(EXACT_MATCH_FILTER),
    REGEXP_MATCH(REG_EXP_MATCH_FILTER),
    GLOB_MATCH(GLOB_EXP_MATCH_FILTER),
    MULTI_MATCH(MULTI_MATCH_FILTER);


    private ConsumeMessageFilter currentMessageFilter;

    ConsumeMessageFilterMode(ConsumeMessageFilter filter){
        currentMessageFilter = filter;
    }

    public ConsumeMessageFilter getFilter() {
        return this.currentMessageFilter;
    }
}
