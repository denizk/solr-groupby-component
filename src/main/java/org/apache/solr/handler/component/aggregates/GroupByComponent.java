package org.apache.solr.handler.component.aggregates;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.lucene.document.FieldType.NumericType;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.join.FixedBitSetCachingWrapperFilter;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;
import org.apache.lucene.util.Version;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.DateUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.request.SimpleFacets;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.TrieDateField;
import org.apache.solr.schema.TrieDoubleField;
import org.apache.solr.schema.TrieFloatField;
import org.apache.solr.schema.TrieIntField;
import org.apache.solr.schema.TrieLongField;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrIndexSearcher;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

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

    private static final Logger log = LoggerFactory.getLogger(GroupByComponent.class);

    public static final String COMPONENT_NAME = "groupby";

    private static final String BLOCK_JOIN_PATH_HINT = "/";

    public static class Params {
        public static final String GROUPBY = "groupby";

        public static final String DEBUG = "groupby.debug";
        
        public static final String STATS = "groupby.stats";
        
        /**
         * By default true, will return the distinct counts at the lowest
         * level in the group by.
         */
        public static final String DISTINCT = "groupby.distinct";
        
        public static final String RANGE = "groupby.range";

        public static final String LIMIT = "groupby.limit";
        
        public static final String SIZE = "groupby.size";
        
        /**
         * If you would like the groupby function to take into account
         * any active filters (fq, q). By default groupby does not take
         * into account filters.
         */
        public static final String FILTER = "groupby.filter";
        
        /**
         * Computes the intersection at all levels given all permutations.
         */
        public static final String PIVOT = "groupby.pivot";
        
        /**
         * If you would like to take each group and interect them at the same
         * level to create a hierarchy representing the set/intersect/union
         * between all possible combinations. (WARNING: will return all data,
         * however, use with distinct to create nice "unique vs. total" counts)
         */
        public static final String INTERSECT = "groupby.intersect";

        /**
         * The minimum count to be returned, by default 1
         */
        public static final String MINCOUNT = "groupby.mincount";
        
        /**
         * Define size/cardinality accuracy (trade speed/memory vs. accuracy) defaults to 12
         */
        public static final String SKETCH_SIZE = "sketch.size";
        
        /**
         * Minimize the results (removing 0 counts, where one can infer). For example
         * performing a range groupby date for an entire year, by default we remove
         * all dates that dont have any data, if you would like to return all data
         * then set this to false
         */
        public static final String MINIMIZE = "groupby.minimize";

        /**
         * Runs the percentiles for the specified field identified in percentiles.
         */
        public static final String PERCENTILES = "groupby.stats.percentiles";

        /**
         * Specifies how much compression to use when generating percentiles using QDigest.
         */
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
        	if (rb.req.getParams().getBool(Params.FILTER, false)) {
        		rb.setNeedDocSet( true );
        	}
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
        DocSet contrained_set_of_documents = rb.req.getParams().getBool(Params.FILTER, false) ? rb.getResults().docSet : null;

        // grab parameters for aggregating against always set facet
        // to max values to allow for distributed queries and for
        // the group by to always return max
        // TODO - "groupby having(*)"
        ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
        params.set("facet", true);
        params.set("facet.limit", req.getParams().getInt(Params.LIMIT, Integer.MAX_VALUE));
        params.set("facet.missing", false);
        params.set("facet.mincount", req.getParams().getInt(Params.MINCOUNT, 1));
        params.set("cache", "false");

        // the group by parameters passed in by the user
        // &groupby=product_brand_name&groupby=product_category_name
        String[] groupByArgs = params.getParams(Params.GROUPBY);

        List<Function<AggregationResult, Boolean>> predicates = null;
        String[] havingArgs = req.getParams().getParams(Params.HAVING);
        if (havingArgs != null && havingArgs.length > 0) {
            predicates = new ArrayList<Function<AggregationResult, Boolean>>();
            for (String k : havingArgs) {
                predicates.add(new AggregateQueryBuilder(k).build());
            }
        }

        NamedList<Object> debug = new SimpleOrderedMap<Object>();

        List<NamedList<Object>> pivot = new ArrayList<NamedList<Object>>();
        for (String groupByArg : groupByArgs) {
            String[] groupByFields = groupByArg.split(",");
            LinkedList<String> queue = new LinkedList<String>();
            queue.addAll(Lists.newArrayList(groupByFields));
            pivot.add(collect(contrained_set_of_documents, queue, req, params, predicates));
        }
        rb.rsp.add("group", pivot);

        if (req.getParams().getBool(Params.DEBUG, false)) {
            rb.rsp.add("groups.debug", debug);
        }
    }

    private SimpleOrderedMap<Object> collect(DocSet contrained_set_of_documents, LinkedList<String> queue, SolrQueryRequest req, SolrParams params, List<Function<AggregationResult, Boolean>> predicates) throws IOException {

        String field = queue.removeFirst();

        NamedList<Integer> facets = null;
        IndexSchema schema = req.getSearcher().getSchema();

        LinkedList<String> parents = new LinkedList<String>();

        // TODO possible optimization, see what is being asked for
        // and limit docset back to just those docs with the implicitly
        // included docs
        DocSet docs = null;
        if (hasBlockJoinHint(field)) {
            String[] blockQueryTerm = field.split(BLOCK_JOIN_PATH_HINT)[0].split(":");
            BooleanQuery q = new BooleanQuery();
            q.add(new TermQuery(new Term(blockQueryTerm[0], blockQueryTerm[1])), Occur.MUST);

            // check if second level query for constraints
            String fieldName = field.split(BLOCK_JOIN_PATH_HINT)[1];
            if (fieldName.indexOf(":") > -1) {
                String[] pair = fieldName.split(":");
                Query query = extractQuery(schema, fieldName, null);
                fieldName = pair[0];
                q.add(query, Occur.MUST);
            }

            docs = req.getSearcher().getDocSet(q);
            if (contrained_set_of_documents != null) {
            	docs = docs.intersection(contrained_set_of_documents);
            }

            parents.add(field);
            facets = doFacets(fieldName, docs, req, params);
        } else {
            docs = req.getSearcher().getDocSet(new WildcardQuery(new Term(field, "*")));
            if (contrained_set_of_documents != null) {
            	docs = docs.intersection(contrained_set_of_documents);
            }
            facets = doFacets(field, docs, req, params);
        }

        SimpleOrderedMap<Object> results = new SimpleOrderedMap<Object>();

        results.add(field, collectChildren(contrained_set_of_documents, schema, field, queue, req, docs, params, facets, parents, predicates));
        
        if (params.getBool(Params.DISTINCT, false) && params.getBool(Params.INTERSECT, true)) {
            intersect(results, params);
        }

        return results;
    }

    
    private NamedList<Integer> doFacets(String fieldName, DocSet docs, SolrQueryRequest req, SolrParams params) throws IOException {
        // check of we are doing range facet
        String start_range = req.getParams().get(Params.RANGE + "." + fieldName + ".start");
        String end_range = req.getParams().get(Params.RANGE + "." + fieldName + ".end");
        String gap = req.getParams().get(Params.RANGE + "." + fieldName + ".gap");
        if (start_range != null && end_range != null && gap != null) {
        	ModifiableSolrParams x = new ModifiableSolrParams(params);
        	x.set("facet", true);
        	x.set("facet.date", fieldName);
        	x.set("facet.date.start", DateMathParserFixed.toIsoFormat(DateMathParserFixed.extract(null, start_range)));
        	x.set("facet.date.end", DateMathParserFixed.toIsoFormat(DateMathParserFixed.extract(null, end_range)));
        	x.set("facet.date.gap", gap);
        	
        	try {
        		// TODO move to range query filter
        		SimpleOrderedMap<Object> ranges = new SimpleOrderedMap<Object>();
        		new SimpleFacets(req, docs, x).getFacetDateCounts(fieldName, ranges);
        		NamedList<Object> dates = (NamedList<Object>)ranges.get(fieldName);
        		
        		// remove metadata from result set
        		dates.remove("end");
        		dates.remove("start");
        		dates.remove("gap");
        		
        		NamedList<Integer> results = new NamedList<Integer>();
        		
        		DateTime start_period = DateMathParserFixed.extract(null, start_range);
        		DateTime end_period = DateMathParserFixed.extract(null, end_range);
        		
        		// build up all dates available
        		DateTime current_start_period = start_period;
        		while (current_start_period.isBefore(end_period)) {
        			
        		    DateMathParserFixed p = new DateMathParserFixed();
        		    p.setNow(current_start_period);
        			DateTime start_date = current_start_period;
        			DateTime stop_date = p.parseMath(gap);        			
        			
        			Object match = null;
        			for (Entry<String, Object> entry : dates) {
        				DateTime dt = DateMathParserFixed.fromIsoFormat(entry.getKey());
						if (start_date.toDate().getTime() <= dt.toDate().getTime() && dt.toDate().getTime() < stop_date.toDate().getTime()) {
							match = entry.getValue();
						}
					}
        			if (match != null) {
        				results.add(fieldName + ":[" + DateMathParserFixed.toIsoFormat(start_date) + " TO " + DateMathParserFixed.toIsoFormat(stop_date) + "]", Integer.parseInt(match.toString()));
        			} else {
        				results.add(fieldName + ":[" + DateMathParserFixed.toIsoFormat(start_date) + " TO " + DateMathParserFixed.toIsoFormat(stop_date) + "]", 0);
        			}
        			current_start_period = stop_date;
        		}
        		
        		return results;
        	} catch (Exception ex) {
        		throw new RuntimeException(ex);
        	}
        } else {
        	return new SimpleFacets(req, docs, params).getTermCounts(fieldName);
        }
    }
    
    @SuppressWarnings("unchecked")
	private void intersect(final SimpleOrderedMap<Object> results, SolrParams params) {
        try {
            for (Entry<String, Object> entry : results) {
                if (entry.getValue() instanceof List<?>) {
                    collectHLL(null, (List<NamedList<Object>>)entry.getValue(), params);
                    clean((List<NamedList<Object>>)entry.getValue());
                }
            }
        } catch (CardinalityMergeException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
        	throw new RuntimeException(e);
        }
    }
    
    private static interface HyperLogLogCollector {
    	void collect(NamedList<Object> item, HyperLogLog hll) throws CardinalityMergeException;
    }
    
    private void iterateHLL(List<NamedList<Object>> items, HyperLogLogCollector collector) {
    	// merge them up and across parents
        for (NamedList<Object> item : items) {
			// iterate and cross join
			if (item.get("hll") != null) {
				HyperLogLog hll = (HyperLogLog)item.get("hll");
				try {
					collector.collect(item, hll);
				} catch (CardinalityMergeException ex) {
					throw new RuntimeException(ex);
				}
			}
		}
    }
    
    private class HyperLogLogUnion {
    	HyperLogLog root;
    	HashMap<String, HyperLogLog> children = new HashMap<String, HyperLogLog>();
    	HashMap<String, NamedList<Object>> child_nodes = new HashMap<String, NamedList<Object>>();
    	NamedList<Object> node;
    }
    
    private void clean(final List<NamedList<Object>> list) {
    	for (NamedList<Object> p : list) {
    		if (p.get("group") != null) {
    			Object o = p.get("group");
    			if (o instanceof List<?>) {
    				clean((List<NamedList<Object>>)o);
            	} else if (o != null) {
            		NamedList<Object> v = (NamedList<Object>)o;
	                for (Entry<String, Object> entry : v) {
						if (entry.getValue() instanceof List<?>) {
							clean((List<NamedList<Object>>)entry.getValue());
						}
					}
					v.remove("hll");
					v.remove("xyz");
					v.remove("ancestor");
					if (v.get("join") != null) {
						NamedList<Object> join = (NamedList<Object>)v.get("join");
						if (join.iterator().hasNext()) {
							NamedList<Object> details = ((NamedList<Object>)join.getVal(0));
							details.remove("hll");
							details.remove("xyz");
							details.remove("ancestor");
						}
					}
            	} else {
            		System.out.println(o);
            	}
    		}
			p.remove("hll");
			p.remove("xyz");
			p.remove("ancestor");
			if (p.get("join") != null) {
				NamedList<Object> join = (NamedList<Object>)p.get("join");
				join.remove("hll");
				join.remove("xyz");
				join.remove("ancestor");
			}
    	}
    }
    
    // walk tree and collect all HLL matrix to create intersection (possible to do intersects at every level) which
    // could be great... pivot {A,B,C} => intersects at C level, B level, and A level
    @SuppressWarnings("unchecked")
	private List<NamedList<Object>> collectHLL(final NamedList<Object> parent, final List<NamedList<Object>> list, final SolrParams params) throws CardinalityMergeException, IOException {
        HashMap<NamedList<Object>, HyperLogLog> matrix = new HashMap<NamedList<Object>, HyperLogLog>();
        
        final HashMap<String, NamedList<Object>> nodesBYName = new HashMap<String, NamedList<Object>>();
        
        List<NamedList<Object>> collectHLL = new ArrayList<NamedList<Object>>();
        for (NamedList<Object> p : list) {
        	nodesBYName.put(p.get("value").toString(), p);
            if (p.get("group") != null) {
            	Object o = p.get("group");
            	if (o instanceof List<?>) {
            		collectHLL.addAll(collectHLL(p, (List<NamedList<Object>>)o, params));
            	} else if (o != null) {
	                // found it
	                NamedList<Object> v = (NamedList<Object>)o;
	                for (Entry<String, Object> entry : v) {
						if (entry.getValue() instanceof List<?>) {
							collectHLL.addAll(collectHLL(p, (List<NamedList<Object>>)entry.getValue(), params));
						}
					}
	                if (v.get("hll") != null) {
	                	v.add("xyz", p.get("value").toString());
	                    HyperLogLog x = (HyperLogLog)v.get("hll");
	                    matrix.put(v, x);
	                    if (parent != null) {
	                    	p.add("ancestor", parent.get("value").toString());
	                    }
	                    collectHLL.add(p);
	                }
            	}
            }
        }
        
        if (params.getBool(Params.PIVOT, false)) {
	        final HashMap<String, HyperLogLogUnion> pivot = new HashMap<String, HyperLogLogUnion>();
	        iterateHLL(collectHLL, new HyperLogLogCollector() {
				public void collect(NamedList<Object> item, HyperLogLog hll) throws CardinalityMergeException {
					String key = item.get("ancestor").toString();
					String facet = item.get("xyz").toString();
					if (false == pivot.containsKey(key)) {
						pivot.put(key, new HyperLogLogUnion());
					}
					HyperLogLogUnion union = pivot.get(key);
					union.root = union.root != null ? (HyperLogLog)union.root.merge(hll) : hll;
					union.node = nodesBYName.get(key);
					if (item.get("join") != null) {
						NamedList<Object> node = (NamedList<Object>)item.get("join");
						if (node.get(key) != null) {
							NamedList<Object> x = (NamedList<Object>)node.get(key);
							Object hllObj = x.get("hll");
							if (hllObj != null) {
								union.children.put(facet, (HyperLogLog)hllObj);
								union.child_nodes.put(key, x);
							}
						}
					}
					if (item.get("hll") != null) {
						union.children.put(facet, (HyperLogLog)item.get("hll"));
					}
					union.child_nodes.put(facet, item);
				}
			});
	        // we now have distincts at each level, time to cross-multiply them
	        for (String key_a : pivot.keySet()) {
	        	HyperLogLogUnion setA = pivot.get(key_a);
	        	long count_of_a = setA.root.cardinality();
	        	NamedList<Object> sets = new NamedList<Object>();
				// now we have top most parent which contains group[] array
				// we get root intersect/merge
				for (String key_b : pivot.keySet()) {
					if (key_a == key_b) {
						continue;
					}
					HyperLogLogUnion setB = pivot.get(key_b);
					long count_of_b = setB.root.cardinality();
					long union_a_b = setA.root.merge(setB.root).cardinality();
					long intersection_a_b = (count_of_a + count_of_b) - union_a_b;
					NamedList<Object> set = new NamedList<Object>();
					set.add("intersect", intersection_a_b);
					set.add("union", union_a_b);
					set.add("total", count_of_a + count_of_b);
					sets.add(key_b, set);
					
					Set<String> covered = Sets.newHashSet();
					for (String entry : setA.children.keySet()) {
						HyperLogLog set_a_child = setA.children.get(entry);
						long count_of_a_child = set_a_child.cardinality();
						NamedList<Object> child_sets = new NamedList<Object>();
						
						if (setA.child_nodes.get(entry).get("pivot") != null) {
							child_sets = ((NamedList<Object>)setA.child_nodes.get(entry).get("pivot"));
						} else {
							setA.child_nodes.get(entry).add("pivot", child_sets);
						}
						
						if (setB.children.containsKey(entry)) {
							HyperLogLog set_b_child = setB.children.get(entry);
							long count_of_b_child = set_b_child.cardinality();
							long union_of_a_child_b_child = set_a_child.merge(set_b_child).cardinality();
							long intersection_of_a_child_b_child = (count_of_a_child + count_of_b_child) - union_of_a_child_b_child;

							NamedList<Object> child_set = new NamedList<Object>();
							child_set.add("intersect", intersection_of_a_child_b_child);
							child_set.add("union", union_of_a_child_b_child);
							child_set.add("total", count_of_a_child + count_of_b_child);
							child_sets.add(key_b, child_set);
						}
						covered.add(entry);
					}
					for (String entry : setB.children.keySet()) {
						if (covered.contains(entry)) continue;
						// setA does not contain what is in setB, so we can safely zero it all out
						System.out.println("zero out " + entry);
					}
				}
				setA.node.add("pivot", sets);
			}
        }
        
        HashMap<NamedList<Object>, SimpleOrderedMap<Object>> sets = new HashMap<NamedList<Object>, SimpleOrderedMap<Object>>();
        
        // now with matrix if it has anything build intersections
        for (NamedList<Object> a : matrix.keySet()) {
        	SimpleOrderedMap<Object> wrap = new SimpleOrderedMap<Object>();
            for (NamedList<Object> b : matrix.keySet()) {
                if (a == b) continue;
                HyperLogLog hll_A = matrix.get(a);
                HyperLogLog hll_B = matrix.get(b);
                if (hll_A == null || hll_B == null) {
                	continue;
                }
                
                // are there zeros on either side if so we can just assume empty hll and move on
                if ((Integer)a.get("total") <= 0 || (Integer)b.get("total") <= 0) {
	                NamedList<Object> set = new NamedList<Object>();
	                set.add("intersect", 0);
	                set.add("union", (Integer)a.get("total") + (Integer)b.get("total"));
	                set.add("total", (Integer)a.get("total") + (Integer)b.get("total"));
	                wrap.add(b.get("xyz").toString(), set);
                } else {
	                HyperLogLog union = (HyperLogLog)hll_A.merge(hll_B);
	                long union_count = union.cardinality();
	                long total_count = matrix.get(a).cardinality() + matrix.get(b).cardinality();
	                long inclusion_exclusion_principle_instersect = total_count - union_count;
	                NamedList<Object> set = new NamedList<Object>();
	                set.add("intersect", inclusion_exclusion_principle_instersect);
	                set.add("union", union_count);
	                set.add("total", total_count);
	                set.add("hll", union);
	                wrap.add(b.get("xyz").toString(), set);
                }
            }
            sets.put(a, wrap);
        }
        
        List<NamedList<Object>> yield = new ArrayList<NamedList<Object>>();
        for (NamedList<Object> item : sets.keySet()) {
        	item.add("join", sets.get(item));
        	if (parent != null) {
        		item.add("ancestor", parent.get("value").toString());
        	}
        	yield.add(item);
		}
        return yield;
    }

    @SuppressWarnings("unchecked")
    private List<NamedList<Object>> collectChildren(DocSet contrained_set_of_documents, IndexSchema schema, String parentField, LinkedList<String> queue, SolrQueryRequest req, DocSet docs, SolrParams params, NamedList<Integer> parents, LinkedList<String> priorQueries, List<Function<AggregationResult, Boolean>> predicates) throws IOException {
        List<NamedList<Object>> results = new ArrayList<NamedList<Object>>(parents.size());
        int HLL_SKETCH_SIZE = params.getInt(Params.SKETCH_SIZE, 12);

        String nextField = queue.pollFirst();
        SolrIndexSearcher indexSearcher = req.getSearcher();

        for (Map.Entry<String, Integer> parent : parents) {	// facets to iterate over (date ranges inclusive)
            if (parent.getValue() <= 0 && params.getBool(Params.MINIMIZE, true)) {
                continue; // do not collect children when parent is 0
            }

            SimpleOrderedMap<Object> pivot = new SimpleOrderedMap<Object>();
            if (parent.getKey().contains(":") && parent.getKey().contains(" TO ")) {
            	pivot.add("value", parent.getKey());
            	String key = parent.getKey().substring(parent.getKey().indexOf(":")+1).replace("[", "").replace("]", "");
            	String[] ab = key.split(" TO ");
            	pivot.add("range.start", ab[0]);
            	pivot.add("range.stop", ab[1]);
            } else {
            	pivot.add("value", parent.getKey());
            }
            pivot.add("count", parent.getValue());
            
            boolean skip = false;
            
            if (params.getParams(Params.STATS) != null) {
                NamedList<Object> stats = new NamedList<Object>();

                for (String statField : params.getParams(Params.STATS)) {
                    String statFieldName = statField;

                    Query statQuery = getNestedBlockJoinQueryOrTermQuery(schema, parentField, parent.getKey(), priorQueries, statField);
                    DocSet statDocs = indexSearcher.getDocSet(statQuery);
                    if (contrained_set_of_documents != null) {
                    	statDocs = statDocs.intersection(contrained_set_of_documents);
                    }

                    if (hasBlockJoinHint(statFieldName)) {
                        statFieldName = statFieldName.split(BLOCK_JOIN_PATH_HINT)[1].split(":")[0];
                    }

                    AggregationResult percentiles = new Aggregate(req, statDocs, statFieldName).sum();
                    if (predicates != null) {
                        for (Function<AggregationResult, Boolean> f : predicates) {
                            Boolean match = f.apply(percentiles);
                            if (match != null && false == match) {
                                skip = true;
                                break;
                            }
                        }
                        if (skip) {
                            break;
                        }
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
                Query constrainQuery = getNestedBlockJoinQueryOrTermQuery(schema, parentField, parent.getKey(), priorQueries, nextField);
                DocSet intersection = indexSearcher.getDocSet(constrainQuery);
                if (contrained_set_of_documents != null) {
                	intersection = intersection.intersection(contrained_set_of_documents);
                }

                NamedList<Integer> children;
                if (hasBlockJoinHint(nextField)) {
                    String fieldName = nextField.split(BLOCK_JOIN_PATH_HINT)[1];
                    // has constraint filter
                    if (fieldName.indexOf(":") > -1) {
                        String[] filter = fieldName.split(":");
                        Query sub = extractQuery(schema, fieldName, null);
                        fieldName = filter[0];
                        children = doFacets(fieldName, indexSearcher.getDocSet(sub, intersection), req, params); //new SimpleFacets(req, indexSearcher.getDocSet(sub, intersection), params).getTermCounts(fieldName);
                    } else {
                        children = doFacets(fieldName, intersection, req, params);// new SimpleFacets(req, intersection, params).getTermCounts(fieldName);
                    }

                } else {
                    children = doFacets(nextField, intersection, req, params);//new SimpleFacets(req, intersection, params).getTermCounts(nextField);
                }

                if (children.size() >= 0) {
                    LinkedList<String> clone = new LinkedList<String>(priorQueries);
                    if (hasBlockJoinHint(parentField)) {
                        clone.add(parentField.split(BLOCK_JOIN_PATH_HINT)[0] + "/" + parentField.split(BLOCK_JOIN_PATH_HINT)[1].split(":")[0] + ":" + parent.getKey());
                    } else {
                    	// are we a range query?
                    	if (parent.getKey().matches("^.*:\\[.*\\sTO\\s.*\\]$")) {
                    		clone.add(parent.getKey());
                    	} else {
                    		clone.add(parentField.split(":")[0] + ":" + parent.getKey() + "/" + parentField.split(":")[0] + ":" + parent.getKey());
                    	}
                    }
                    
                    // check if we have distinct, and if so, are we last item? if so, then only return unique items
                    if (params.getParams(Params.DISTINCT) != null && queue.size() <= 0) {
                    	// count them up in a rough sketch
                    	HyperLogLog hll = new HyperLogLog(HLL_SKETCH_SIZE);
                    	Integer count = 0;
                    	for (Map.Entry<String, Integer> child : children) {
                    		hll.offer(child.getKey());
                    		count += child.getValue();
                    	}
                    	NamedList<Object> n = new NamedList<Object>();
                    	n.add("unique", children.size() <= 0 ? 0 : hll.cardinality());
                    	n.add("total", count);
                    	if (params.getBool(Params.INTERSECT, true)) {
                    		n.add("hll", hll);
                    	}
                    	String fieldName = nextField;
                        if (hasBlockJoinHint(fieldName)) {
                        	fieldName = fieldName.split(BLOCK_JOIN_PATH_HINT)[1].split(":")[0];
                        }
                        n.add("field", fieldName);
                        pivot.add("group", n);

                    } else {
                    	NamedList<Object> n = new NamedList<Object>();
                    	n.add(nextField, collectChildren(contrained_set_of_documents, schema, nextField, (LinkedList<String>) queue.clone(), req, intersection, params, children, clone, predicates));                   	
                    	pivot.add("group", n);
                    }
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
    private static boolean hasBlockJoinHint(String field) {
        if (field == null)
            return false;
        return field.indexOf(BLOCK_JOIN_PATH_HINT) > -1;
    }

    private static String extractBlockJoinHint(String query) {
        if (hasBlockJoinHint(query))
            return query.split(BLOCK_JOIN_PATH_HINT)[0];
        return null;
    }
    
    private static Date tryParseDate(String query) {
        try {
            return DateUtil.parseDate(query);
        } catch (ParseException e) {
            return null;
        }
    }
    
    private static Query buildQueryFromText(String field, IndexSchema schema, String query) {
        QueryParser queryParser = new QueryParser(Version.LUCENE_45, field, schema.getQueryAnalyzer());
        queryParser.setAllowLeadingWildcard(false);
        queryParser.setLowercaseExpandedTerms(false);
        queryParser.setTimeZone(TimeZone.getDefault());
        queryParser.setAnalyzeRangeTerms(true);
        try {
            return queryParser.parse(query);
        } catch (org.apache.lucene.queryparser.classic.ParseException e) {
            throw new RuntimeException(e);
        }
    }

    private static Query extractQuery(IndexSchema schema, String term, String value) {
        String field = term;
        if (hasBlockJoinHint(term)) {
            field = term.split(BLOCK_JOIN_PATH_HINT)[1];
        }
        if (field.indexOf(":") > -1) {
            String[] keyValue = field.split(":");
            field = keyValue[0];
            String query = keyValue[1];
            return buildQueryFromText(field, schema, query);
        } else if (value.matches("^.*:\\[.*\\sTO\\s.*\\]$")) {
        	return buildQueryFromText(field, schema, value);
        } else if (null != tryParseDate(value)) {
            if (schema.getField(field).getType() instanceof TrieDateField) {
                return new TrieDateField().getFieldQuery(null, schema.getField(field), value);
            }
            throw new RuntimeException("Can not group on date field not a TrieDateField");
        } else if (value.matches("^-{0,1}[0-9]+")) {
        	// number
        	FieldType type = schema.getField(field).getType();
        	NumericType numericType = type.getNumericType();
        	if (numericType == NumericType.FLOAT) {
        		return new TrieFloatField().getFieldQuery(null, schema.getField(field), value);
        	} else if (numericType == NumericType.INT) {
        		return new TrieIntField().getFieldQuery(null, schema.getField(field), value);
        	} else if (numericType == NumericType.LONG) {
        		return new TrieLongField().getFieldQuery(null, schema.getField(field), value);
        	} else if (numericType == NumericType.DOUBLE) {
        		return new TrieDoubleField().getFieldQuery(null, schema.getField(field), value);
        	} else {
        		return new WildcardQuery(new Term(field, null != value ? value : "*"));
        	}
        } else {
            return new WildcardQuery(new Term(field, null != value ? value : "*"));
        }
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
    private Query getNestedBlockJoinQueryOrTermQuery(IndexSchema schema, String termKey, String termValue, LinkedList<String> previousQueries, String nextField) {

        LinkedHashMap<String, HashSet<String>> blockJoins = new LinkedHashMap<String, HashSet<String>>();

        for (String priorQuery : previousQueries) {
            String hint = extractBlockJoinHint(priorQuery);
            if (hint != null) {
                if (!blockJoins.containsKey(hint)) {
                    blockJoins.put(hint, new HashSet<String>());
                }
                if (priorQuery.split(BLOCK_JOIN_PATH_HINT)[1].indexOf(":") > -1) {
                    // remove all block joins at this level (we have most specific already)
                    blockJoins.get(hint).clear();
                    blockJoins.get(hint).add(priorQuery);
                } else {
                    blockJoins.get(hint).add(hint);
                }
            }
        }

        String childBlockJoinHint = extractBlockJoinHint(nextField);
        // check that we aren't on same level as child...
        int i = previousQueries.size() - 1;
        String previousJoinHint = null;
        Boolean hasBlockJoinInQueryHieararchy = false;
        while (previousJoinHint == null && i >= 0) {
        	if (!hasBlockJoinInQueryHieararchy && previousQueries.get(i).split(BLOCK_JOIN_PATH_HINT).length > 1) {
        		String[] pair = previousQueries.get(i).split(BLOCK_JOIN_PATH_HINT);
        		hasBlockJoinInQueryHieararchy = !pair[0].equalsIgnoreCase(pair[1]);
        	}
            previousJoinHint = extractBlockJoinHint(previousQueries.get(i));
            
            i = i - 1;
        }
        
        // does any prior actuall have a block join?
        if (previousJoinHint != null && previousJoinHint.matches("^[^:]+:[^\\/]+$") && hasBlockJoinInQueryHieararchy == false) {
        	previousJoinHint = null;
        }

        BooleanQuery query = new BooleanQuery();
        if (null != previousJoinHint) {
            if (previousJoinHint.equalsIgnoreCase(childBlockJoinHint)) {
                // we are querying at same level
                for (String fq : blockJoins.get(previousJoinHint)) {
                    query.add(extractQuery(schema, fq, null), Occur.MUST);
                }
                query.add(extractQuery(schema, termKey, termValue), Occur.MUST);
            } else {
                if (previousJoinHint.equalsIgnoreCase(extractBlockJoinHint(termKey))) {
                    // this query being executed as at same level as prior parent
                    // can we can assume more restrictive than parent?
                    BooleanQuery q = new BooleanQuery();
                    Query thisQuery = extractQuery(schema, termKey, termValue);
                    q.add(extractQuery(schema, previousJoinHint, null), Occur.MUST);
                    CachingWrapperFilter filter = new FixedBitSetCachingWrapperFilter(new QueryWrapperFilter(q));
                    query.add(new ToChildBlockJoinQuery(thisQuery, filter, false), Occur.MUST);
                } else {
                    BooleanQuery bq = new BooleanQuery();

                    // sanitize block joins if we have hints grab most specific and move on
                    // noun:shopper vs noun:shopper/id:12341234 we should only use id:12341234

                    for (String joinKey : blockJoins.keySet()) {
                        if (joinKey.equalsIgnoreCase(extractBlockJoinHint(termKey))) {
                            for (String fq : blockJoins.get(joinKey)) {
                                bq.add(extractQuery(schema, fq, null), Occur.MUST);
                            }
                            bq.add(extractQuery(schema, termKey, termValue), Occur.MUST);
                        } else if (joinKey.equalsIgnoreCase(extractBlockJoinHint(nextField))) {
                            // ignore
                        } else {
                            BooleanQuery n = new BooleanQuery();
                            for (String fq : blockJoins.get(joinKey)) {
                                n.add(extractQuery(schema, fq, null), Occur.MUST);
                            }
                            bq.add(n, Occur.MUST);
                        }
                    }

                    Query scope = extractQuery(schema, previousJoinHint, "");

                    CachingWrapperFilter filter = new FixedBitSetCachingWrapperFilter(new QueryWrapperFilter(scope));
                    Query join = new ToChildBlockJoinQuery(bq, filter, false);

                    query.add(join, Occur.MUST);
                    query.add(extractQuery(schema, termKey, termValue), Occur.MUST);
                }
            }
        } else if (null != childBlockJoinHint && previousJoinHint == null) {
            // this is first time we are looking at a child and
            // we can assume everything before this has been a parent query
            BooleanQuery q = new BooleanQuery();
            for (String key : blockJoins.keySet()) {
                for (String fq : blockJoins.get(key)) {
                    q.add(extractQuery(schema, fq, null), Occur.MUST);
                }
            }
            CachingWrapperFilter filter = new FixedBitSetCachingWrapperFilter(new QueryWrapperFilter(q));
            query.add(new ToChildBlockJoinQuery(extractQuery(schema, termKey, termValue), filter, false), Occur.MUST);
        } else {
            // no block join hints specified build regular solr query
            for (String key : blockJoins.keySet()) {
                for (String fq : blockJoins.get(key)) {
                    query.add(extractQuery(schema, fq, null), Occur.MUST);
                }
            }
            query.add(extractQuery(schema, termKey, termValue), Occur.MUST);
        }
        return query;

    }
}
