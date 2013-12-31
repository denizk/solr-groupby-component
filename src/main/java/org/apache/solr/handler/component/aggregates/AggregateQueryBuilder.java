package org.apache.solr.handler.component.aggregates;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

import com.google.common.base.Function;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Builds the function that is used to match documents.
 */
public class AggregateQueryBuilder {

    private static final Pattern rangeQueryPattern = Pattern.compile("(.+):\\[([\\-0-9\\.\\*]+)\\sTO\\s([\\-0-9\\.\\*]+)\\]");

    public final Float start;

    public final Float stop;

    public final String fieldName;

    public AggregateQueryBuilder(String query) {
        Matcher matcher = rangeQueryPattern.matcher(query.toUpperCase());
        if (false == matcher.find()) {
            throw new SolrException(ErrorCode.BAD_REQUEST, "Could not parse range query for having filter");
        }
        fieldName = matcher.group(1);
        this.start = matcher.group(2).equals("*") ? null : Float.parseFloat(matcher.group(2));
        this.stop = matcher.group(3).equals("*") ? null : Float.parseFloat(matcher.group(3));
    }

    public Function<AggregationResult, Boolean> build() {
        return new Function<AggregationResult, Boolean>() {
            public Boolean apply(AggregationResult value) {
                // this is not the field you are looking for...
                if (!value.getField().equalsIgnoreCase(fieldName)) {
                    return null;
                }
                if (start == null && stop == null) {
                    // [* TO *] (N)
                    return true;
                } else if (start == null && stop != null) {
                    // [* TO #] (N <=)
                    if (value.getSum() <= stop)
                        return true;
                } else if (start != null && stop == null) {
                    // [# TO *] (N >=)
                    if (value.getSum() >= start) {
                        return true;
                    }
                } else if (value.getSum() >= start && value.getSum() <= stop) {
                    // [# TO $] (# <= N >= #)
                    return true;
                }
                return false;
            }
        };
    }
}
