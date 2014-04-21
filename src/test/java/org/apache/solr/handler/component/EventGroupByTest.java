package org.apache.solr.handler.component;

import java.io.IOException;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.handler.component.aggregates.GroupByComponent;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.NodeList;

/**
 * Tests the group by component on a flat index.
 * 
 * @author Terrance A. Snyder
 *
 */
public class EventGroupByTest extends SolrTestCaseJ4 {
	
    @Override
    public void setUp() throws Exception {
        super.setUp();
        clearIndex();
        setupIndex();
    }
    
    @BeforeClass
    public static void beforeTests() throws Exception {
        initCore("solrconfig-aggregates.xml", "schema-events.xml");
    }
    
    @Test
    public void should_be_able_to_pivot() throws Exception {
        ModifiableSolrParams p = new ModifiableSolrParams();
        p.set("q", "*:*");
        p.set("wt", "xml");
        p.set("rows", "0");
        p.set("indent", "true");
        p.set(GroupByComponent.Params.GROUPBY, "network_id,site_id,type,cid");
        SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), p);
        String xml = h.query(req);
        System.out.println(xml);
    }
    
    @Test
    public void should_be_able_to_pivot_distinct() throws Exception {
        ModifiableSolrParams p = new ModifiableSolrParams();
        p.set("q", "*:*");
        p.set("wt", "xml");
        p.set("rows", "0");
        p.set("indent", "true");
        p.set(GroupByComponent.Params.GROUPBY, "network_id,site_id,type,cid");
        p.set(GroupByComponent.Params.DISTINCT, "true");
        SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), p);
        String xml = h.query(req);
        System.out.println(xml);
        
        // we should have 2 unique shoppers
        assertEquals(XPathHelper.query(xml, "//arr[@name='network_id']").getLength(), 1);
        assertEquals(XPathHelper.query(xml, "//arr[@name='site_id']").getLength(), 1);
        assertEquals(XPathHelper.query(xml, "//arr[@name='site_id']").getLength(), 1);
        assertEquals(XPathHelper.query(xml, "//int[@name='media_delivery']").getLength(), 1);
        assertEquals(XPathHelper.query(xml, "//int[@name='conversion']").getLength(), 1);
        
        assertEquals(XPathHelper.getText(xml, "//int[@name='conversion']/..//long[@name='unique']"), "1");
        assertEquals(XPathHelper.getText(xml, "//int[@name='media_delivery']/..//long[@name='unique']"), "2");
    }
    
    @Test
    public void should_be_able_to_pivot_distinct_on_multivalue_field() throws Exception {
        ModifiableSolrParams p = new ModifiableSolrParams();
        p.set("q", "*:*");
        p.set("wt", "xml");
        p.set("rows", "0");
        p.set("indent", "true");
        p.set(GroupByComponent.Params.GROUPBY, "source_ids,cid");
        p.set(GroupByComponent.Params.DISTINCT, "true");
        SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), p);
        String xml = h.query(req);
        System.out.println(xml);
      

        assertEquals(XPathHelper.query(xml, "//arr[@name='source_ids']").getLength(), 1);
        assertEquals(XPathHelper.query(xml, "//int[@name='111111']").getLength(), 1);
        assertEquals(XPathHelper.query(xml, "//int[@name='222222']").getLength(), 1);
        assertEquals(XPathHelper.query(xml, "//int[@name='2222222']").getLength(), 1);
        assertEquals(XPathHelper.query(xml, "//int[@name='333333']").getLength(), 1);
        assertEquals(XPathHelper.query(xml, "//int[@name='0000000']").getLength(), 1);
        
        assertEquals(XPathHelper.getText(xml, "//int[@name='111111']/..//long[@name='unique']"), "1");
        assertEquals(XPathHelper.getText(xml, "//int[@name='111111']/..//int[@name='total']"), "2");
        assertEquals(XPathHelper.getText(xml, "//int[@name='222222']/..//long[@name='unique']"), "1");
        assertEquals(XPathHelper.getText(xml, "//int[@name='222222']/..//int[@name='total']"), "2");
    }
    
    @Test
    public void should_be_able_to_pivot_distinct_with_base_query() throws Exception {
        ModifiableSolrParams p = new ModifiableSolrParams();
        p.set("q", "cid:88888888");
        p.set("wt", "xml");
        p.set("rows", "0");
        p.set("indent", "true");
        p.set(GroupByComponent.Params.GROUPBY, "type,cid");
        p.set(GroupByComponent.Params.FILTER, "true");
        SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), p);
        String xml = h.query(req);
        System.out.println(xml);
        
        // we should have 2 unique shoppers
        assertEquals(XPathHelper.query(xml, "//arr[@name='type']").getLength(), 1);
        assertEquals(XPathHelper.query(xml, "//int[@name='media_delivery']").getLength(), 1);
        assertEquals(XPathHelper.query(xml, "//int[@name='conversion']").getLength(), 0);
        assertEquals(XPathHelper.query(xml, "//int[@name='media_delivery']/..//int[@name='12341234']").getLength(), 0);
        assertEquals(XPathHelper.query(xml, "//int[@name='media_delivery']/..//int[@name='88888888']").getLength(), 1);
        
        assertEquals(XPathHelper.getText(xml, "//int[@name='media_delivery']/..//int[@name='88888888']"), "1");
    }
    
    protected void setupIndex() throws IOException {
        GroupByComponent c = (GroupByComponent) h.getCore().getSearchComponents().get(GroupByComponent.COMPONENT_NAME);
        assertTrue(c instanceof GroupByComponent);
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", "1");
        doc.addField("type", "conversion");
        doc.addField("cid", 12341234L);
        doc.addField("dt", "2014-01-01T12:00:00Z");
        doc.addField("source_ids", "111111");
        doc.addField("source_ids", "222222");
        doc.addField("source_ids", "333333");
        doc.addField("site_id", "1");
        doc.addField("network_id", "1");
        assertU(adoc(doc));
        
        doc = new SolrInputDocument();
        doc.addField("id", "2");
        doc.addField("type", "media_delivery");
        doc.addField("cid", 12341234L);
        doc.addField("dt", "2014-01-01T11:00:00Z");
        doc.addField("source_ids", "111111");
        doc.addField("source_ids", "222222");
        doc.addField("source_ids", "333333");
        doc.addField("site_id", "1");
        doc.addField("network_id", "1");
        assertU(adoc(doc));
        
        doc = new SolrInputDocument();
        doc.addField("id", "3");
        doc.addField("type", "conversion");
        doc.addField("cid", 12341234L);
        doc.addField("dt", "2014-01-01T11:00:00Z");
        doc.addField("source_ids", "2222222");
        doc.addField("site_id", "1");
        doc.addField("network_id", "1");
        assertU(adoc(doc));
        
        doc = new SolrInputDocument();
        doc.addField("id", "4");
        doc.addField("type", "media_delivery");
        doc.addField("cid", 12341234L);
        doc.addField("dt", "2014-01-01T11:00:00Z");
        doc.addField("source_ids", "2222222");
        doc.addField("site_id", "1");
        doc.addField("network_id", "1");
        assertU(adoc(doc));
        
        doc = new SolrInputDocument();
        doc.addField("id", "5");
        doc.addField("type", "media_delivery");
        doc.addField("cid", 88888888L);
        doc.addField("dt", "2014-01-01T11:00:00Z");
        doc.addField("source_ids", "0000000");
        doc.addField("source_ids", "1111111");
        doc.addField("site_id", "1");
        doc.addField("network_id", "1");
        assertU(adoc(doc));
        
        assertU(commit());
    }
}
