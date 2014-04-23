package org.apache.solr.handler.component;

import java.io.IOException;
import java.io.StringReader;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.xerces.parsers.DOMParser;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class XPathHelper {
    public static NodeList query(String xml, String xpath) throws SAXException, IOException, XPathExpressionException {
        DOMParser parser = new DOMParser();
        parser.parse(new InputSource(new StringReader(xml)));
        Document document = parser.getDocument();
        XPath statement = XPathFactory.newInstance().newXPath();
        NodeList nodes = (NodeList) statement.compile(xpath).evaluate(document, XPathConstants.NODESET);
        return nodes;
    }
    
    public static String getText(String xml, String xpath) {
    	try {
    		return query(xml, xpath).item(0).getTextContent();
    	} catch (Exception ex) {
    		throw new RuntimeException("Could not process/find node '" + xpath + "'", ex);
    	}
    }
    
    public static Long getLong(String xml, String xpath) throws SAXException, IOException, XPathExpressionException {
    	return Long.parseLong(getText(xml, xpath));
    }
}
