package com.youzan.nsq.client.entity;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by lin on 17/11/20.
 */
public interface ConsumeMessageFilter<T, V> {

    boolean apply(T filter, V filterVal);

    int getType();

    ConsumeMessageFilter EXACT_MATCH_FILTER = new ConsumeMessageFilter<String, String>() {

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

    ConsumeMessageFilter REG_EXP_MATCH_FILTER = new ConsumeMessageFilter<Pattern, String>() {

        @Override
        public boolean apply(Pattern patternFilter, String filterVal) {
            if(null == patternFilter) {
                return true;
            }
            if (patternFilter.matcher(filterVal).matches()) {
                return true;
            } else {
                return false;
            }
        }

        @Override
        public int getType() {
            return 2;
        }
    };

    ConsumeMessageFilter GLOB_EXP_MATCH_FILTER = new ConsumeMessageFilter<PathMatcher, String>() {

        @Override
        public boolean apply(PathMatcher filter, String filterVal) {
            if(null == filter) {
                return true;
            }
            Path path = Paths.get(filterVal);
            if (filter.matches(path)) {
                return true;
            } else {
                return false;
            }
        }

        @Override
        public int getType() {
            return 3;
        }
    };

    ConsumeMessageFilter MULTI_MATCH_FILTER = new ConsumeMessageFilter<Pair, Map>() {

        @Override
        public boolean apply(Pair filter, Map extJson) {
            Pair<NSQConfig.MultiFiltersRelation, List<Pair<String, String>>> multiFilters = (Pair<NSQConfig.MultiFiltersRelation, List<Pair<String, String>>>) filter;
            if(null == multiFilters) {
                return true;
            }
            NSQConfig.MultiFiltersRelation relation = multiFilters.getKey();
            Map<String, String> extJsonHeader = (Map<String, String>) extJson;
            boolean match = true;
            for (Pair<String, String> filterData : multiFilters.getRight()) {
                String extVal = extJsonHeader.get(filterData.getKey());
                match = EXACT_MATCH_FILTER.apply(filterData.getRight(), extVal);
                if (match && relation == NSQConfig.MultiFiltersRelation.OR) {
                    return true;
                }
                if (!match && relation == NSQConfig.MultiFiltersRelation.AND) {
                    return false;
                }
            }
            return match;
        }

        @Override
        public int getType() {
            return 3;
        }
    };
}

