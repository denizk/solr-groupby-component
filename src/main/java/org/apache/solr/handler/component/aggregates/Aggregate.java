package org.apache.solr.handler.component.aggregates;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SimpleFacets;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DocSet;

import com.clearspring.analytics.stream.quantile.QDigest;
import com.google.common.util.concurrent.FutureCallback;

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

public class Aggregate {

    private DocSet base;

    private String fieldName;

    private SolrQueryRequest req;

    private boolean doPercentiles;

    private Integer[] requestedPercentiles;

    private double compression;

    public Aggregate(SolrQueryRequest req, DocSet base, String fieldName) {
        this.base = base;
        this.fieldName = fieldName;
        this.req = req;

        String percentiles = req.getParams().get(GroupByComponent.Params.PERCENTILES, "");
        if (percentiles != null && percentiles.isEmpty() == false) {
            this.doPercentiles = true;
            List<Integer> list = new ArrayList<Integer>();
            for (String p : percentiles.split(",")) {
                list.add(Integer.parseInt(p));
            }
            this.requestedPercentiles = list.toArray(new Integer[] {});
            this.compression = req.getParams().getDouble(GroupByComponent.Params.PERCENTILES_COMPRESSION, 100);
        }
    }

    public AggregationResult sum() throws IOException {
        return sum(fieldName);
    }

    private AggregationResult sum(String fieldName) throws IOException {
        ModifiableSolrParams p = new ModifiableSolrParams(this.req.getParams());
        p.set("facet.mincount", 1);
        SimpleFacets facets = new SimpleFacets(this.req, base, p);

        NamedList<Integer> terms = facets.getTermCounts(fieldName, base);

        Double sum = 0D;
        Double itemValue = 0D;
        Long count = 0L;

        final AggregationResult result = new AggregationResult(fieldName);
        FutureCallback<Double> cb = null;
        if (doPercentiles) {
            result.setQdigest(new QDigest(this.compression));
            result.setPercentiles(this.requestedPercentiles);
            cb = new FutureCallback<Double>() {
                public void onSuccess(Double value) {
                    result.getQdigest().offer(value.longValue());
                }
                public void onFailure(Throwable arg0) {
                }
            };
        }

        for (Map.Entry<String, Integer> parent : terms) {
            itemValue = Double.parseDouble(parent.getKey());
            sum += itemValue * parent.getValue();
            count += parent.getValue();
            if (cb != null) {
                cb.onSuccess(itemValue);
            }
        }

        result.setSum(sum);
        result.setCount(count);
        return result;
    }
}
