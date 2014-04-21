package org.apache.solr.handler.component;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.handler.component.aggregates.GroupByComponent;
import org.apache.solr.handler.component.test.Consumer;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.xerces.parsers.DOMParser;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.google.gson.Gson;

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

public class GroupByComponentTest extends SolrTestCaseJ4 {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        clearIndex();
        setupIndex();
    }

    @BeforeClass
    public static void beforeTests() throws Exception {
        initCore("solrconfig-aggregates.xml", "schema-aggregates.xml");
    }

    /**
     * A simple test to verify that we can actually facet on a simple field and get back all values
     * across all documents, this is the same as faceting in regular solr.
     * 
     * @throws Exception
     */
    @Test
    public void testBasicGroupBy() throws Exception {
        ModifiableSolrParams p = new ModifiableSolrParams();
        p.set("q", "*:*");
        p.set("wt", "xml");
        p.set("rows", "0");
        p.set("indent", "true");
        p.set(GroupByComponent.Params.GROUPBY, "_root_");
        SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), p);
        String response = h.query(req);
        System.out.println(response);

        DOMParser parser = new DOMParser();
        parser.parse(new InputSource(new StringReader(response)));
        Document document = parser.getDocument();
        XPath xpath = XPathFactory.newInstance().newXPath();
        NodeList nodes = (NodeList) xpath.compile("//arr[@name=\"groups\"]//arr[@name=\"_root_\"]//lst//int").evaluate(document, XPathConstants.NODESET);
        assertEquals(2, nodes.getLength());
        assertEquals("6", nodes.item(0).getTextContent());
        assertEquals("5", nodes.item(1).getTextContent());
    }

    /**
     * Test verifies that if we ask for a specific hierarchy facet we only get facets at that level.
     * In the example below we 'hint' to the group by component we only want the "noun:shopper"
     * level _root_ fields (_root_ field exists at shopper, order, and transaction levels). This
     * will give us a count of "unique" shoppers by simply iterating over any document which is
     * "noun:shopper" (top level) and return the _root_ field at that level by doing a
     * {@link ToParentBlockJoinQuery}.
     * 
     * @throws Exception
     */
    @Test
    public void testBasicGroupByThatSpecifiesBlockJoin() throws Exception {
        ModifiableSolrParams p = new ModifiableSolrParams();
        p.set("q", "*:*");
        p.set("wt", "xml");
        p.set("rows", "0");
        p.set("indent", "true");
        p.set(GroupByComponent.Params.GROUPBY, "noun:shopper/_root_");
        SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), p);
        String xml = h.query(req);
        System.out.println(xml);

        NodeList nodes = xpath(xml, "//arr[@name=\"groups\"]//arr[@name=\"noun:shopper/_root_\"]//lst//int");
        assertEquals(2, nodes.getLength());
        assertEquals("1", nodes.item(0).getTextContent());
        assertEquals("1", nodes.item(1).getTextContent());
    }

    /**
     * Validate we can rollup purchase amount by all shoppers.
     * 
     * @throws Exception
     */
    @Test
    public void testSimpleGroupByWithRollup() throws Exception {
        ModifiableSolrParams p = new ModifiableSolrParams();
        p.set("q", "*:*");
        p.set("wt", "xml");
        p.set("rows", "0");
        p.set("indent", "true");
        p.set(GroupByComponent.Params.GROUPBY, "noun:shopper/_root_");
        p.set(GroupByComponent.Params.STATS, "noun:xact/product_purchase_amount");
        SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), p);
        String xml = h.query(req);
        System.out.println(xml);

        NodeList nodes = xpath(xml, "//arr[@name=\"groups\"]//arr[@name=\"noun:shopper/_root_\"]//lst//int");
        assertEquals(2, nodes.getLength());
        assertEquals("1", nodes.item(0).getTextContent());
        assertEquals("1", nodes.item(1).getTextContent());

        nodes = xpath(xml, "//int[@name=\"11111111\"]/../lst[@name=\"stats\"]/lst[@name=\"product_purchase_amount\"]/double[@name=\"sum\"]");
        assertEquals("6.99", nodes.item(0).getTextContent());
    }

    /**
     * Test out assumption that if someone doesn't provide join hints that it can infer what is
     * going on by falling back to classic SOLR type faceting.
     * 
     * @throws Exception
     */
    @Test
    public void testSimpleGroupByWithRollupWithoutBlockJoinHint() throws Exception {
        ModifiableSolrParams p = new ModifiableSolrParams();
        p.set("q", "*:*");
        p.set("wt", "xml");
        p.set("rows", "0");
        p.set("indent", "true");
        p.set(GroupByComponent.Params.GROUPBY, "_root_");
        p.set(GroupByComponent.Params.STATS, "product_purchase_amount");
        SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), p);
        String xml = h.query(req);
        System.out.println(xml);

        NodeList nodes = xpath(xml, "//arr[@name=\"groups\"]//arr[@name=\"_root_\"]//lst//int");
        assertEquals(2, nodes.getLength());

        nodes = xpath(xml, "//int[@name=\"11111111\"]/../lst[@name=\"stats\"]/lst[@name=\"product_purchase_amount\"]/double[@name=\"sum\"]");
        assertEquals("6.99", nodes.item(0).getTextContent());
    }

    /**
     * We can't "guess" more than if the top level GROUP BY is not prefixed it assumes TOP LEVEL
     * query or at least same level as child. In this case we are stat fields on purchase_amount but
     * we are faceting by city name (different levels). So we must give the hint to SOLR about what
     * we are querying.
     * 
     * @throws Exception
     */
    @Test
    public void testSimpleGroupByWithRollupWithoutBlockJoinHint2() throws Exception {
        ModifiableSolrParams p = new ModifiableSolrParams();
        p.set("q", "*:*");
        p.set("wt", "xml");
        p.set("rows", "0");
        p.set("indent", "true");
        p.set(GroupByComponent.Params.GROUPBY, "noun:order/order_city_name");
        p.set(GroupByComponent.Params.STATS, "noun:xact/product_purchase_amount");
        SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), p);
        String xml = h.query(req);
        System.out.println(xml);

        NodeList nodes = xpath(xml, "//arr[@name=\"groups\"]//arr[@name=\"noun:order/order_city_name\"]//lst//int");
        assertEquals(2, nodes.getLength());

        nodes = xpath(xml, "//int[@name=\"TAMPA\"]/../lst[@name=\"stats\"]/lst[@name=\"product_purchase_amount\"]/double[@name=\"sum\"]");
        assertEquals("9.97", nodes.item(0).getTextContent());

        nodes = xpath(xml, "//int[@name=\"MIAMI\"]/../lst[@name=\"stats\"]/lst[@name=\"product_purchase_amount\"]/double[@name=\"sum\"]");
        assertEquals("2.99", nodes.item(0).getTextContent());
    }

    /**
     * In this example we specify we only want facets at the lowest purchase level, returning the
     * distinct # of purchases made by each consumer regardless of # of orders.
     * 
     * @throws Exception
     */
    @Test
    public void testBasicGroupByThatSpecifiesBlockJoinAtLowestLevelExplictly() throws Exception {
        ModifiableSolrParams p = new ModifiableSolrParams();
        p.set("q", "*:*");
        p.set("wt", "xml");
        p.set("rows", "0");
        p.set("indent", "true");
        p.set(GroupByComponent.Params.GROUPBY, "noun:order/order_city_name,noun:xact/product_brand_name");
        SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), p);
        String xml = h.query(req);
        System.out.println(xml);

        NodeList nodes = xpath(xml, "//int[@name=\"TAMPA\"]/../arr/lst/int[@name=\"STARBURST\"]");
        assertEquals(1, nodes.getLength());
        assertEquals("1", nodes.item(0).getTextContent());
    }

    /**
     * Test that we can implicitly guess the child object such that we can specify 'noun:order' and
     * not have to specify the children as it is implied.
     * 
     * @throws Exception
     */
    @Test
    public void testBasicGroupByThatSpecifiesBlockJoinAtLowestLevelImplicitly() throws Exception {
        ModifiableSolrParams p = new ModifiableSolrParams();
        p.set("q", "*:*");
        p.set("wt", "xml");
        p.set("rows", "0");
        p.set("indent", "true");
        p.set(GroupByComponent.Params.GROUPBY, "noun:order/order_city_name,noun:xact/product_brand_name");
        SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), p);
        String xml = h.query(req);
        System.out.println(xml);

        // verify that the total # of star bursts returned for TAMPA is 1 (as we have multiple
        // starbursts in multi cities, but only 1 in TAMPA)
        NodeList nodes = xpath(xml, "//int[@name=\"TAMPA\"]/../arr/lst/int[@name=\"STARBURST\"]");
        assertEquals(1, nodes.getLength());
        assertEquals("1", nodes.item(0).getTextContent());
    }

    /**
     * More complex nested group by giving extra emphasis on the block join hints. Here we want to
     * create a pivot of: [state]->[city]->[category].
     * 
     * @throws Exception
     */
    @Test
    public void testNestedGroupBy() throws Exception {
        ModifiableSolrParams p = new ModifiableSolrParams();
        p.set("q", "*:*");
        p.set("wt", "xml");
        p.set("rows", "0");
        p.set("indent", "true");
        p.set(GroupByComponent.Params.GROUPBY, "noun:order/order_state_name,noun:order/order_city_name,noun:xact/product_category_name");
        SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), p);
        String xml = h.query(req);
        System.out.println(xml);

        NodeList nodes = xpath(xml, "//int[@name=\"TAMPA\"]/../arr/lst/int[@name=\"ENERGY DRINKS\"]");

        assertEquals(1, nodes.getLength());
        assertEquals("3", nodes.item(0).getTextContent());

        // verify tampa only has the 4 categories of purchase
        nodes = xpath(xml, "//int[@name=\"TAMPA\"]/../arr/lst/int");
        assertEquals(3, nodes.getLength());

        // verify sunrise as 2 categories
        nodes = xpath(xml, "//int[@name=\"MIAMI\"]/../arr/lst/int");
        assertEquals(1, nodes.getLength());
    }

    /**
     * More complex nested group by giving extra emphasis on the block join hints. Here we want to
     * create a pivot of: [state]->[city]->[category] AND we also want to add in stats at all levels
     * for product_purchase_amount.
     * 
     * @throws Exception
     */
    @Test
    public void testSumGroupBy() throws Exception {
        ModifiableSolrParams p = new ModifiableSolrParams();
        p.set("q", "*:*");
        p.set("wt", "xml");
        p.set("rows", "0");
        p.set("indent", "true");
        p.set(GroupByComponent.Params.GROUPBY, "noun:order/order_state_name,noun:order/order_city_name:TAMPA,noun:xact/product_category_name");
        p.set(GroupByComponent.Params.STATS, "noun:xact/product_purchase_amount");
        SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), p);
        String xml = h.query(req);
        System.out.println(xml);

        // rollup at TAMPA should be $10.00
        NodeList nodes = xpath(xml, "//str[text()=\"TAMPA\"]/../lst[@name=\"stats\"]/lst[@name=\"product_purchase_amount\"]/double[@name=\"sum\"]");
        assertEquals(1, nodes.getLength());
        assertEquals("9.97", nodes.item(0).getTextContent());
        // rollup for energy drinks in TAMPA should be $7.00
        nodes = xpath(xml, "//str[text()=\"TAMPA\"]//arr[@name=\"noun:xact/product_category_name\"]//str[text()=\"ENERGY DRINKS\"]/../lst[@name=\"stats\"]/lst[@name=\"product_purchase_amount\"]/double[@name=\"sum\"]");
        assertEquals(1, nodes.getLength());
        assertEquals("6.97", nodes.item(0).getTextContent().substring(0, 4));
    }

    /**
     * Ensure that we can pivot over date fields and get a resonable result.
     * 
     * @throws Exception
     */
    @Test
    public void testGroupByDateAndSum() throws Exception {
        ModifiableSolrParams p = new ModifiableSolrParams();
        p.set("q", "*:*");
        p.set("wt", "xml");
        p.set("rows", "0");
        p.set("indent", "true");
        p.set(GroupByComponent.Params.GROUPBY, "noun:order/order_date");
        p.set(GroupByComponent.Params.STATS, "noun:xact/product_purchase_amount");
        SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), p);
        String xml = h.query(req);
        System.out.println(xml);

        NodeList nodes = xpath(xml, "//int[@name=\"2013-04-07T12:00:00Z\"]/../lst/lst/double[@name=\"sum\"]");
        assertEquals(1, nodes.getLength());
        assertEquals("6.99", nodes.item(0).getTextContent());

        nodes = xpath(xml, "//int[@name=\"2013-05-14T12:00:00Z\"]/../lst/lst/double[@name=\"sum\"]");
        assertEquals(1, nodes.getLength());
        assertEquals("2.99", nodes.item(0).getTextContent());

        nodes = xpath(xml, "//int[@name=\"2013-05-22T12:00:00Z\"]/../lst/lst/double[@name=\"sum\"]");
        assertEquals(1, nodes.getLength());
        assertEquals("2.98", nodes.item(0).getTextContent());
    }

    /**
     * Sanity check, no customizations, simple facet over purchase amount.
     * 
     * @throws Exception
     */
    @Test
    public void testSanityFacetCount() throws Exception {
        ModifiableSolrParams p = new ModifiableSolrParams();
        p.set("q", "*:*");
        p.set("wt", "xml");
        p.set("rows", "0");
        p.set("indent", "true");
        p.set("facet.limit", Integer.MAX_VALUE);
        p.set("facet.mincount", 1);
        p.set("facet", true);
        p.set("facet.field", "product_purchase_amount");
        SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), p);
        String xml = h.query(req);
        System.out.println(xml);

        // if regular facets over entire index we should get 3:$5, 2:$2, 1:$1
        NodeList nodes = xpath(xml, "//lst[@name=\"product_purchase_amount\"]/int");
        assertEquals(5, nodes.getLength());
        assertEquals("2", nodes.item(0).getTextContent());
        assertEquals("2.0", nodes.item(0).getAttributes().getNamedItem("name").getTextContent());

        assertEquals("1", nodes.item(1).getTextContent());
        assertEquals("1.0", nodes.item(1).getAttributes().getNamedItem("name").getTextContent());

        assertEquals("1", nodes.item(2).getTextContent());
        assertEquals("1.99", nodes.item(2).getAttributes().getNamedItem("name").getTextContent());
    }

    /**
     * Sanity check, no customizations, simple facet over purchase amount.
     * 
     * @throws Exception
     */
    @Test
    public void testSanityFacetCountWithFilter() throws Exception {
        ModifiableSolrParams p = new ModifiableSolrParams();
        p.set("q", "*:*");
        p.set("wt", "xml");
        p.set("rows", "0");
        p.set("indent", "true");
        p.set("facet.limit", Integer.MAX_VALUE);
        p.set("facet.mincount", 1);
        p.set("facet", true);
        p.set("fq", "product_category_name:\"ENERGY DRINKS\"");
        p.set("facet.field", "product_purchase_amount");
        SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), p);
        String xml = h.query(req);
        System.out.println(xml);

        // there should be 3 $5 energy drinks and 1 $2.0 energy drink
        NodeList nodes = xpath(xml, "//lst[@name=\"product_purchase_amount\"]/int");
        assertEquals(4, nodes.getLength());
        assertEquals("1", nodes.item(0).getTextContent());
        assertEquals("1.99", nodes.item(0).getAttributes().getNamedItem("name").getTextContent());

        assertEquals("1", nodes.item(1).getTextContent());
        assertEquals("2.0", nodes.item(1).getAttributes().getNamedItem("name").getTextContent());

        assertEquals("1", nodes.item(2).getTextContent());
        assertEquals("2.98", nodes.item(2).getAttributes().getNamedItem("name").getTextContent());

        assertEquals("1", nodes.item(3).getTextContent());
        assertEquals("2.99", nodes.item(3).getAttributes().getNamedItem("name").getTextContent());
    }

    /**
     * We need a way to tell solr to filter out some items when doing a group by. This is basic
     * filtering and 'should' be provided by plain ol' "q" and "fq" logic. However when querying
     * multiple levels with nested hierarchies SOLR only goes up or down the stack not both so it is
     * very difficult to think of the "q" and "fq" required to make it work.
     * 
     * Rather than forcing it, this is an alternate way to get there (read: probably not the
     * optimal) as it allows someone to constrain the results inline, and it reads left to right
     * reasonably well like xpath.
     * 
     * @throws Exception
     */
    @Test
    public void testSumGroupByWithSubQuery() throws Exception {
        ModifiableSolrParams p = new ModifiableSolrParams();
        p.set("q", "*:*");
        p.set("wt", "xml");
        p.set("rows", "0");
        p.set("indent", "true");
        // group by also allows for constraint to be added (regular fq syntax)
        p.set(GroupByComponent.Params.GROUPBY, "noun:order/order_state_name:FLORIDA,noun:order/order_city_name:TAMPA,noun:xact/product_category_name");
        SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), p);
        String xml = h.query(req);
        System.out.println(xml);

        // verify we only pull back the constrained queries
        NodeList nodes = xpath(xml, "//arr[@name=\"groups\"]/lst/arr");
        assertEquals(1, nodes.getLength());
        nodes = xpath(xml, "//int[@name=\"FLORIDA\"]");
        assertEquals(1, nodes.getLength());
        nodes = xpath(xml, "//int[@name=\"TAMPA\"]");
        assertEquals(1, nodes.getLength());
        nodes = xpath(xml, "//int[@name=\"ENERGY DRINKS\"]");
        assertEquals(1, nodes.getLength());
        nodes = xpath(xml, "//int[@name=\"CANDY\"]");
        assertEquals(1, nodes.getLength());
        nodes = xpath(xml, "//int[@name=\"PROTEIN SHAKE\"]");
        assertEquals(1, nodes.getLength());
    }

    /**
     * Mimics functionality found in https://issues.apache.org/jira/browse/SOLR-3583, albeit it
     * doesn't handle custom facets and facet ranges on YEAR, MONTH, DATE. Allows for variable
     * setting of percentiles accuracy based on 'compression'. And allows for multiple combinations.
     * 
     * Works distributed through the use of data analytics/sketch library for clearspring.
     * Specifically QDigest.
     * 
     * @throws Exception
     */
    @Test
    public void testPercentiles() throws Exception {
        ModifiableSolrParams p = new ModifiableSolrParams();
        p.set("q", "order_date:A*");
        p.set("wt", "xml");
        p.set("rows", "0");
        p.set("indent", "true");
        p.set(GroupByComponent.Params.GROUPBY, "noun:order/noun,noun:order/order_date");
        p.add(GroupByComponent.Params.STATS, "noun:xact/product_purchase_amount");
        p.set(GroupByComponent.Params.PERCENTILES, "25,50,75");
        p.set(GroupByComponent.Params.PERCENTILES_COMPRESSION, "1000");
        p.set(GroupByComponent.Params.DEBUG, true);
        SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), p);
        String xml = h.query(req);
        System.out.println(xml);

        NodeList nodes = xpath(xml, "//int[@name=\"order\"]/../lst[@name=\"stats\"]/lst[@name=\"product_purchase_amount\"]/double[@name=\"sum\"]");
        assertEquals(1, nodes.getLength());
        assertEquals("12.96", nodes.item(0).getTextContent());
    }

    /**
     * Here we test that a single group by with a conditional clause that returns exactly one rooted
     * document aggregates up to the correct level. This mimics a 'group by shopper and sum(amount)'
     * command.
     * 
     * @throws Exception
     */
    @Test
    public void testPercentilesRooted() throws Exception {
        ModifiableSolrParams p = new ModifiableSolrParams();
        p.set("q", "*:*");
        p.set("wt", "xml");
        p.set("rows", "0");
        p.set("indent", "true");
        p.set(GroupByComponent.Params.GROUPBY, "noun:shopper/id:11111111");
        p.add(GroupByComponent.Params.STATS, "noun:xact/product_purchase_amount");
        p.set(GroupByComponent.Params.PERCENTILES, "25,50,75");
        p.set(GroupByComponent.Params.PERCENTILES_COMPRESSION, "1000");
        SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), p);
        String xml = h.query(req);
        System.out.println(xml);

        NodeList nodes = xpath(xml, "//int[@name=\"11111111\"]/../lst[@name=\"stats\"]/lst[@name=\"product_purchase_amount\"]/double[@name=\"sum\"]");
        assertEquals(1, nodes.getLength());
        assertEquals("6.99", nodes.item(0).getTextContent());
    }

    /**
     * Just a test for developer information and debugging to see how cleanly the JSON response is
     * when generated.
     * 
     * @throws Exception
     */
    @Test
    public void testJsonFormat() throws Exception {
        ModifiableSolrParams p = new ModifiableSolrParams();
        p.set("q", "*:*");
        p.set("wt", "json");
        p.set("json.nl", "map");
        p.set("rows", "0");
        p.set("indent", "true");
        p.set(GroupByComponent.Params.GROUPBY, "noun:shopper/id:44444444");
        p.add(GroupByComponent.Params.STATS, "noun:xact/product_purchase_amount");
        p.set(GroupByComponent.Params.PERCENTILES, "25,50,75");
        p.set(GroupByComponent.Params.PERCENTILES_COMPRESSION, "1000");
        SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), p);
        String json = h.query(req);
        System.out.println(json);
    }

    @Test
    public void testComplexDrillPath() throws Exception {
        ModifiableSolrParams p = new ModifiableSolrParams();
        p.set("q", "*:*");
        p.set("wt", "xml");
        p.set("rows", "0");
        p.set("indent", "true");
        p.set(GroupByComponent.Params.GROUPBY, "noun:shopper/id,noun:order/order_state_name:F*,noun:order/order_city_name:TAMPA,noun:xact/product_brand_name:R*");
        p.add(GroupByComponent.Params.STATS, "noun:xact/product_purchase_amount");
        p.set(GroupByComponent.Params.PERCENTILES, "25,50,75");
        p.set(GroupByComponent.Params.PERCENTILES_COMPRESSION, "1000");
        SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), p);
        String xml = h.query(req);
        System.out.println(xml);
    }
    
    @Test
    public void testGroupByDistinctPivot() throws Exception {
        ModifiableSolrParams p = new ModifiableSolrParams();
        p.set("q", "*:*");
        p.set("wt", "xml");
        p.set("rows", "0");
        p.set("indent", "true");
        p.set("cache", "false");
        p.set("fq", "id:111111*");
        p.set(GroupByComponent.Params.GROUPBY, "noun:shopper/id,noun:xact/product_brand_name");
        p.set(GroupByComponent.Params.FILTER, true);
        // p.set(GroupByComponent.Params.DISTINCT, "true");
        SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), p);
        String xml = h.query(req);
        System.out.println(xml);
    }

    /**
     * Expected to fail...
     * 
     * @throws Exception
     */
    @Test
    public void testPredicate() throws Exception {
        ModifiableSolrParams p = new ModifiableSolrParams();
        p.set("q", "*:*");
        p.set("wt", "xml");
        p.set("rows", "0");
        p.set("indent", "true");
        p.set("cache", "false");
        p.set(GroupByComponent.Params.GROUPBY, "noun:shopper/id,noun:xact/product_brand_name:\"RED BULL\"");
        p.add(GroupByComponent.Params.STATS, "noun:xact/product_purchase_amount");
        SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), p);
        String xml = h.query(req);
        System.out.println(xml);

        NodeList nodes = xpath(xml, "//int[@name=\"11111111\"]/../arr/lst/lst/lst/double[@name=\"sum\"]");
        assertEquals(1, nodes.getLength());
        assertEquals("1.99", nodes.item(0).getTextContent());

        nodes = xpath(xml, "//int[@name=\"22222222\"]/../arr/lst/lst/lst/double[@name=\"sum\"]");
        assertEquals(1, nodes.getLength());
        assertEquals("5.97", nodes.item(0).getTextContent().substring(0, 4));
    }

    /**
     * Expected to fail...
     * 
     * @throws Exception
     */
    @Test
    public void testWildcardDrillThrough() throws Exception {
        ModifiableSolrParams p = new ModifiableSolrParams();
        p.set("q", "*:*");
        p.set("wt", "xml");
        p.set("rows", "0");
        p.set("indent", "true");
        p.set(GroupByComponent.Params.GROUPBY, "noun:shopper/id,noun:xact/product_brand_name:R*");
        p.add(GroupByComponent.Params.STATS, "noun:xact/product_purchase_amount");
        SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), p);
        String xml = h.query(req);
        System.out.println(xml);

        NodeList nodes = xpath(xml, "//arr[@name=\"noun:xact/product_brand_name:R*\"]/lst/lst/lst/double[@name=\"sum\"]");
        assertEquals(2, nodes.getLength());
        assertEquals("1.99", nodes.item(0).getTextContent());
        assertEquals("5.97", nodes.item(1).getTextContent().substring(0, 4));
    }

    private NodeList xpath(String xml, String xpath) throws SAXException, IOException, XPathExpressionException {
        DOMParser parser = new DOMParser();
        parser.parse(new InputSource(new StringReader(xml)));
        Document document = parser.getDocument();
        XPath statement = XPathFactory.newInstance().newXPath();
        NodeList nodes = (NodeList) statement.compile(xpath).evaluate(document, XPathConstants.NODESET);
        return nodes;
    }

    private SolrInputDocument getConsumerDocument(String file) throws IOException {
        InputStream stream = this.getClass().getResourceAsStream(file);
        SolrInputDocument doc = new Gson().fromJson(new InputStreamReader(stream), Consumer.class).asDocument();
        return doc;
    }

    protected void setupIndex() throws IOException {
        GroupByComponent c = (GroupByComponent) h.getCore().getSearchComponents().get(GroupByComponent.COMPONENT_NAME);
        assertTrue(c instanceof GroupByComponent);
        assertU(adoc(getConsumerDocument("/sample1.json")));
        assertU(adoc(getConsumerDocument("/sample2.json")));
        // assertU(adoc(getConsumerDocument("/sample3.json")));
        assertU(commit());
    }
}
