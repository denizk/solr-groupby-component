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

public class ClickToConversionTests extends SolrTestCaseJ4 {
	
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
    public void should_be_able_to_intersect_and_cross_join() throws Exception {
        ModifiableSolrParams p = new ModifiableSolrParams();
        p.set("q", "*:*");
        p.set("wt", "json");
        p.set("json.nl", "map");
        p.set("rows", "0");
        p.set("indent", "true");
        p.set(GroupByComponent.Params.GROUPBY, "event,dt,user_id");
        p.set(GroupByComponent.Params.DISTINCT, "true");
        p.set(GroupByComponent.Params.PIVOT, "true");
        
        p.set(GroupByComponent.Params.RANGE + ".dt.start", "2014-01-01T00:00:00Z/DAY");
        p.set(GroupByComponent.Params.RANGE + ".dt.end", "2014-01-1T00:00:00Z/DAY+14DAYS");
        p.set(GroupByComponent.Params.RANGE + ".dt.gap", "+7DAY");
        
        SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), p);
        String xml = h.query(req);
        System.out.println(xml);
    }
    
    protected void setupIndex() throws IOException {
        GroupByComponent c = (GroupByComponent) h.getCore().getSearchComponents().get(GroupByComponent.COMPONENT_NAME);
        assertTrue(c instanceof GroupByComponent);
    	
    	SolrInputDocument doc = new SolrInputDocument();
        doc = new SolrInputDocument();
        doc.addField("id", "1");
        doc.addField("event", "click");
        doc.addField("user_id", "12341234");
        doc.addField("dt", "2014-01-01T12:32:12Z");
        assertU(adoc(doc));
        
        doc = new SolrInputDocument();
        doc.addField("id", "2");
        doc.addField("event", "impression");
        doc.addField("user_id", "12341234");
        doc.addField("dt", "2014-01-01T12:32:13Z");
        assertU(adoc(doc));
        
        doc = new SolrInputDocument();
        doc.addField("id", "3");
        doc.addField("event", "conversion");
        doc.addField("user_id", "12341234");
        doc.addField("dt", "2014-01-10T15:23:13Z");
        assertU(adoc(doc));
        
        assertU(commit());
    }
}
