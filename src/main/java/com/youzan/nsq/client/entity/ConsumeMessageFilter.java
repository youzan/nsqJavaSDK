package com.youzan.nsq.client.entity;

import org.apache.commons.lang3.StringUtils;

/**
 * Created by lin on 17/11/20.
 */
public interface ConsumeMessageFilter {
    ConsumeMessageFilter EXACT_MATCH_FILTER = new ConsumeMessageFilter() {

        @Override
        public boolean apply(String filter, String filterVal) {
            if(StringUtils.isBlank(filter)) {
                return true;
            } else if (filter.equals(filterVal)) {
                return true;
            } else {
                return false;
            }
        }

        @Override
        public int getType() {
            return 1;
        }


    };

    boolean apply(String filter, String filterVal);

    int getType();
}
