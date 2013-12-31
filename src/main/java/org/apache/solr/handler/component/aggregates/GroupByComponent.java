package org.apache.solr.handler.component.aggregates;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.join.FixedBitSetCachingWrapperFilter;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.request.SimpleFacets;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

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

public class GroupByComponent extends SearchComponent {

    private static Logger log = LoggerFactory.getLogger(GroupByComponent.class);

    public static final String COMPONENT_NAME = "groupby";

    private static final String BLOCK_JOIN_PATH_HINT = "/";

    public static class Params {
        public static final String GROUPBY = "groupby";

        public static final String STATS = "groupby.stats";

        public static final String PERCENTILES = "groupby.stats.percentiles";

        public static final String PERCENTILES_COMPRESSION = "groupby.stats.percentiles.compression";

        /**
         * Parameter used for filtering out results based upon range queries. For example
         * '?groupby.having=sum(product_purchase_amount):[10 * 100]'. Only a small subset of
         * aggregate queries are supported at this time.
         */
        public static final String HAVING = "groupby.having";
    }

    @Override
    public void prepare(ResponseBuilder rb) throws IOException {
        if (rb.req.getParams().get(Params.GROUPBY, "").isEmpty() == false) {
            rb.setNeedDocSet(true);
            if (log.isDebugEnabled()) {
                log.debug("Activated GroupByComponent");
            }
        }
    }

    @Override
    public void process(ResponseBuilder rb) throws IOException {
        if (rb.req.getParams().get(Params.GROUPBY, "").isEmpty()) {
            return;
        }

        SolrQueryRequest req = rb.req;
        DocSet docs = rb.getResults().docSet;

        // grab parameters for aggregating against always set facet
        // to max values to allow for distributed queries and for
        // the group by to always return max
        // TODO - "groupby having(*)"
        ModifiableSolrParams params = new ModifiableSolrParams(rb.req.getParams());
        params.set("facet", true);
        params.set("facet.limit", Integer.MAX_VALUE);
        params.set("facet.missing", false);
        params.set("facet.mincount", 1);

        // the group by parameters passed in by the user
        // &groupby=product_brand_name&groupby=product_category_name
        String[] groupByArgs = params.getParams(Params.GROUPBY);
        
        List<Function<AggregationResult, Boolean>> predicates = null;
        String[] havingArgs = req.getParams().getParams(Params.HAVING);
        if (havingArgs != null && havingArgs.length > 0) {
            predicates = new ArrayList<Function<AggregationResult,Boolean>>();
            for (String k : havingArgs) {
                predicates.add(new AggregateQueryBuilder(k).build());
            }
        }

        List<NamedList<Object>> pivot = new ArrayList<NamedList<Object>>();
        for (String groupByArg : groupByArgs) {
            String[] groupByFields = groupByArg.split(",");
            LinkedList<String> queue = new LinkedList<String>();
            queue.addAll(Lists.newArrayList(groupByFields));
            pivot.add(collect(queue, req, docs, params, predicates));
        }
        rb.rsp.add("groups", pivot);
    }

    private SimpleOrderedMap<Object> collect(LinkedList<String> queue, SolrQueryRequest req, DocSet docs, SolrParams params, List<Function<AggregationResult, Boolean>> predicates) throws IOException {
        String field = queue.removeFirst();

        NamedList<Integer> facets = null;

        ArrayList<String> parents = new ArrayList<String>();

        if (hasBlockJoinHint(field)) {
            String[] blockQueryTerm = field.split(BLOCK_JOIN_PATH_HINT)[0].split(":");
            BooleanQuery q = new BooleanQuery();
            q.add(new TermQuery(new Term(blockQueryTerm[0], blockQueryTerm[1])), Occur.MUST);

            // check if second level query for constraints
            String fieldName = field.split(BLOCK_JOIN_PATH_HINT)[1];
            if (fieldName.indexOf(":") > -1) {
                String[] pair = fieldName.split(":");
                fieldName = pair[0];
                q.add(new TermQuery(new Term(pair[0], pair[1])), Occur.MUST);
            }

            DocSet intersection = req.getSearcher().getDocSet(q, docs);

            parents.add(blockQueryTerm[0] + ":" + blockQueryTerm[1]);
            facets = new SimpleFacets(req, intersection, params).getTermCounts(fieldName);
        } else {
            facets = new SimpleFacets(req, docs, params).getTermCounts(field, docs);
        }

        SimpleOrderedMap<Object> results = new SimpleOrderedMap<Object>();

        results.add(field, collectChildren(field, queue, req, docs, params, facets, parents, predicates));

        return results;
    }

    @SuppressWarnings("unchecked")
    private List<NamedList<Object>> collectChildren(String parentField, LinkedList<String> queue, SolrQueryRequest req, DocSet docs, SolrParams params, NamedList<Integer> parents, List<String> priorQueries, List<Function<AggregationResult, Boolean>> predicates) throws IOException {
        List<NamedList<Object>> results = new ArrayList<NamedList<Object>>(parents.size());

        String nextField = queue.pollFirst();
        SolrIndexSearcher indexSearcher = req.getSearcher();

        for (Map.Entry<String, Integer> parent : parents) {
            if (parent.getValue() <= 0) {
                continue; // do not collect children when parent is 0
            }

            SimpleOrderedMap<Object> pivot = new SimpleOrderedMap<Object>();
            pivot.add(parent.getKey(), parent.getValue());
            
            boolean skip = false;
            if (params.getParams(Params.STATS) != null) {
                NamedList<Object> stats = new NamedList<Object>();
                for (String statField : params.getParams(Params.STATS)) {
                    String statFieldName = statField;
                    if (hasBlockJoinHint(statFieldName)) {
                        statFieldName = statFieldName.split(BLOCK_JOIN_PATH_HINT)[1];
                    }
                    Query statQuery = getNestedBlockJoinQueryOrTermQuery(parentField, parent.getKey(), priorQueries, statField);
                    DocSet statDocs = indexSearcher.getDocSet(statQuery);
                    AggregationResult percentiles = new Aggregate(req, statDocs, statFieldName).sum();
                    if (predicates != null) {
                        for (Function<AggregationResult, Boolean> f : predicates) {
                            Boolean match = f.apply(percentiles);
                            if (match != null && false == match) {
                                skip = true;
                                break;
                            }
                        }
                        if (skip) { break; }
                    }
                    NamedList<Object> n = new NamedList<Object>();
                    if (percentiles.getSum() != null) {
                        n.add("sum", percentiles.getSum());
                        n.add("count", percentiles.getCount());
                    }
                    if (percentiles.getQdigest() != null && percentiles.getCount() > 0) {
                        for (Integer i : percentiles.getPercentiles()) {
                            n.add("percentile-" + i, percentiles.getQdigest().getQuantile(i.doubleValue() / 100D));
                        }
                    }
                    stats.add(statFieldName, n);
                }
                if (skip) { 
                    continue;
                }
                pivot.add("stats", stats);
            }

            if (nextField != null) {
                Query constrainQuery = getNestedBlockJoinQueryOrTermQuery(parentField, parent.getKey(), priorQueries, nextField);
                DocSet intersection = indexSearcher.getDocSet(constrainQuery);

                NamedList<Integer> children;
                if (hasBlockJoinHint(nextField)) {
                    String fieldName = nextField.split(BLOCK_JOIN_PATH_HINT)[1];
                    // has constraint filter
                    if (fieldName.indexOf(":") > -1) {
                        String[] filter = fieldName.split(":");
                        Query sub = new TermQuery(new Term(filter[0], filter[1]));
                        intersection = indexSearcher.getDocSet(sub, intersection);
                        fieldName = filter[0];
                    }
                    children = new SimpleFacets(req, intersection, params).getTermCounts(fieldName, intersection);
                } else {
                    children = new SimpleFacets(req, intersection, params).getTermCounts(nextField, intersection);
                }

                if (children.size() >= 0) {
                    List<String> clone = Lists.newArrayList(priorQueries);
                    if (hasBlockJoinHint(parentField)) {
                        clone.add(parentField.split(BLOCK_JOIN_PATH_HINT)[0] + "/" + parentField.split(BLOCK_JOIN_PATH_HINT)[1] + ":" + parent.getKey());
                    } else {
                        clone.add(parentField + ":" + parent.getKey());
                    }
                    pivot.add(nextField, collectChildren(nextField, (LinkedList<String>) queue.clone(), req, intersection, params, children, clone, predicates));
                }
            }
            results.add(pivot);
        }

        return results;
    }

    @Override
    public String getDescription() {
        return "Group By Component";
    }

    @Override
    public String getSource() {
        return "$URL$";
    }

    /**
     * Work around for pivot to allow for specifying the block join to run to get the child facets.
     * 
     * @param field
     *            The field to check for a block join hint.
     * @return True if the field specified for group by has a block join hint.
     */
    private boolean hasBlockJoinHint(String field) {
        if (field == null)
            return false;
        return field.indexOf(BLOCK_JOIN_PATH_HINT) > -1;
    }

    /**
     * Handle when a person wants to pivot over nested document collections.
     * ?groupby=noun:shopper/retailer
     * ,noun:order/order_date,product_category_name,product_brand_name.
     * 
     * Note: Group by assumes groups are in logical nested structure so no need to do weird
     * child->parent->child group by statements.
     * 
     * @param parentFieldName
     * @param parentTermValue
     * @return
     */
    private Query getNestedBlockJoinQueryOrTermQuery(String termKey, String termValue, List<String> previousQueries, String nextField) {

        Set<String> parentBlockJoinHints = new HashSet<String>();
        List<Query> parents = new ArrayList<Query>();
        HashMap<String, List<Query>> blockJoins = new HashMap<String, List<Query>>();

        if (previousQueries.size() > 0) {
            for (String priorQuery : previousQueries) {

                if (hasBlockJoinHint(priorQuery)) {
                    String[] priorJoin = priorQuery.split(BLOCK_JOIN_PATH_HINT);
                    String priorJoinHint = priorJoin[0];
                    String[] priorQueryTerms = priorJoin[1].split(":");

                    parentBlockJoinHints.add(priorJoinHint);
                    if (!blockJoins.containsKey(priorJoinHint)) {
                        blockJoins.put(priorJoinHint, new ArrayList<Query>());
                    }
                    TermQuery parentTermQuery = new TermQuery(new Term(priorQueryTerms[0], priorQueryTerms[1]));
                    parents.add(parentTermQuery);
                    blockJoins.get(priorJoinHint).add(parentTermQuery);
                } else {
                    String[] parentQueryTerms = priorQuery.split(":");
                    TermQuery parentTermQuery = new TermQuery(new Term(parentQueryTerms[0], parentQueryTerms[1]));
                    parents.add(parentTermQuery);
                    parentBlockJoinHints.add(priorQuery);

                    if (!blockJoins.containsKey(priorQuery)) {
                        blockJoins.put(priorQuery, new ArrayList<Query>());
                    }
                    blockJoins.get(priorQuery).add(parentTermQuery);
                }
            }
        }

        Query termQuery;
        String term = termKey;
        if (hasBlockJoinHint(termKey)) {
            String field = termKey.split(BLOCK_JOIN_PATH_HINT)[1];
            term = field;
        }
        if (term.indexOf(":") > -1) { // check has nested select for constraints
            String[] nestedTerms = term.split(":");
            termQuery = new TermQuery(new Term(nestedTerms[0], nestedTerms[1]));
            term = nestedTerms[0];
        }
        if (termValue != null) {
            termQuery = new TermQuery(new Term(term, termValue));
        } else {
            termQuery = new TermQuery(new Term(term));
        }

        if (nextField != null && hasBlockJoinHint(nextField)) {
            String nextFieldHint = nextField.split(BLOCK_JOIN_PATH_HINT)[0];
            if (!parentBlockJoinHints.contains(nextFieldHint)) {
                BooleanQuery wrap = new BooleanQuery();
                for (String blockJoinKey : blockJoins.keySet()) {
                    BooleanQuery parentTerms = new BooleanQuery();
                    for (String q : parentBlockJoinHints) {
                        parentTerms.add(new TermQuery(new Term(q.split(":")[0], q.split(":")[1])), Occur.MUST);
                    }

                    // the current query is at the same level as the next query so we can't run a
                    // child block query
                    // as the next field is on the same level and NOT a child so we keep it at the
                    // current parent level
                    if (hasBlockJoinHint(termKey) && termKey.split(BLOCK_JOIN_PATH_HINT)[0].equalsIgnoreCase(nextFieldHint)) {
                        BooleanQuery child = new BooleanQuery();
                        for (Query q : blockJoins.get(blockJoinKey)) {
                            child.add(q, Occur.MUST);
                        }
                        CachingWrapperFilter filter = new FixedBitSetCachingWrapperFilter(new QueryWrapperFilter(parentTerms));
                        Query join = new ToChildBlockJoinQuery(child, filter, false);
                        wrap.add(join, Occur.MUST);
                        wrap.add(termQuery, Occur.MUST);
                    } else {
                        BooleanQuery child = new BooleanQuery();
                        for (Query q : blockJoins.get(blockJoinKey)) {
                            child.add(q, Occur.MUST);
                        }
                        child.add(termQuery, Occur.MUST);
                        CachingWrapperFilter filter = new FixedBitSetCachingWrapperFilter(new QueryWrapperFilter(parentTerms));
                        Query join = new ToChildBlockJoinQuery(child, filter, false);
                        wrap.add(join, Occur.MUST);
                    }
                }
                return wrap;
            }
        }

        BooleanQuery fq = new BooleanQuery();
        if (previousQueries.size() > 0) {
            for (Query q : parents) {
                fq.add(q, Occur.MUST);
            }
            fq.add(termQuery, Occur.MUST);
        } else {
            fq.add(termQuery, Occur.MUST);
        }
        return fq;
    }
}
