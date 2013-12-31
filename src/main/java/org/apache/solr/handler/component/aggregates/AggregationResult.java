package org.apache.solr.handler.component.aggregates;

import com.clearspring.analytics.stream.quantile.QDigest;

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

public class AggregationResult {

    private Long count;

    private String field;

    private Double sum;

    private QDigest qdigest;

    private Integer[] percentiles;
    
    public AggregationResult() { }
    
    public AggregationResult(String fieldName) {
        this.field = fieldName;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public Double getSum() {
        return sum;
    }

    public void setSum(Double sum) {
        this.sum = sum;
    }

    public QDigest getQdigest() {
        return qdigest;
    }

    public void setQdigest(QDigest qdigest) {
        this.qdigest = qdigest;
    }

    public Integer[] getPercentiles() {
        return percentiles;
    }

    public void setPercentiles(Integer[] percentiles) {
        this.percentiles = percentiles;
    }

}
