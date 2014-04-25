package org.apache.solr.handler.component.stories;

import java.io.IOException;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.handler.component.TimeZoneTestRule;
import org.apache.solr.handler.component.aggregates.GroupByComponent;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.joda.time.DateTimeZone;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

/**
 * Tests the group by component on a flat index.
 * 
 * @author Terrance A. Snyder
 *
 */
public class TotalSpendByCity extends SolrTestCaseJ4 {
	
	@Rule public TestRule timeZoneRule = new TimeZoneTestRule(DateTimeZone.UTC);
	
    @Override
    public void setUp() throws Exception {
        super.setUp();
        clearIndex();
        setupIndex();
    }
    
    @BeforeClass
    public static void beforeTests() throws Exception {
        initCore("solrconfig-aggregates.xml", "schema-wildcard.xml");
    }
    
    @Test
    public void should_be_able_to_get_aggregate_spend_by_city() throws Exception {
        ModifiableSolrParams p = new ModifiableSolrParams();
        p.set("q", "*:*");
        p.set("wt", "json");
        p.set("json.nl", "map");
        p.set("rows", "0");
        p.set("indent", "true");
        p.set(GroupByComponent.Params.GROUPBY, "state,city,category");
        p.set(GroupByComponent.Params.STATS, "spend");
        SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), p);
        String xml = h.query(req);
        System.out.println(xml);
    }
    
    @Test
    public void get_percentile_spend_by_state() throws Exception {
        ModifiableSolrParams p = new ModifiableSolrParams();
        p.set("q", "*:*");
        p.set("wt", "json");
        p.set("json.nl", "map");
        p.set("rows", "0");
        p.set("indent", "true");
        p.set(GroupByComponent.Params.GROUPBY, "state");
        p.set(GroupByComponent.Params.STATS, "spend");
        p.set(GroupByComponent.Params.PERCENTILES, "25,50,75");
        SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), p);
        String xml = h.query(req);
        System.out.println(xml);
    }
    
    protected void setupIndex() throws IOException {
        GroupByComponent c = (GroupByComponent) h.getCore().getSearchComponents().get(GroupByComponent.COMPONENT_NAME);
        assertTrue(c instanceof GroupByComponent);
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", "1");
        doc.addField("state", "FLORIDA");
        doc.addField("city", "TAMPA");
        doc.addField("user_id", "99999");
        doc.addField("spend", 1.50F);
        doc.addField("category", "CAR");
        assertU(adoc(doc));
        
        doc = new SolrInputDocument();
        doc.addField("id", "2");
        doc.addField("state", "FLORIDA");
        doc.addField("city", "ORLANDO");
        doc.addField("user_id", "99999");
        doc.addField("spend", 8.00F);
        doc.addField("category", "CAR");
        assertU(adoc(doc));
        
        doc = new SolrInputDocument();
        doc.addField("id", "3");
        doc.addField("state", "FLORIDA");
        doc.addField("city", "ORLANDO");
        doc.addField("user_id", "11111");
        doc.addField("spend", 1.50F);
        doc.addField("category", "FOOD");
        assertU(adoc(doc));
        
        doc = new SolrInputDocument();
        doc.addField("id", "4");
        doc.addField("state", "FLORIDA");
        doc.addField("city", "ORLANDO");
        doc.addField("user_id", "88888");
        doc.addField("spend", 1.50F);
        doc.addField("category", "FOOD");
        assertU(adoc(doc));
        
        assertU(commit());
    }
}
