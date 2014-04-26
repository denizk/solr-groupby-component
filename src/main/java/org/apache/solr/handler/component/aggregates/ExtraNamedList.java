package org.apache.solr.handler.component.aggregates;

import java.util.HashMap;

import org.apache.solr.common.util.NamedList;

public class ExtraNamedList extends NamedList<Object> {

    private static final long serialVersionUID = 1L;
    
    private HashMap<String, Object> metadata = new HashMap<String, Object>();
    
    public void addMeta(String key, Object value) {
        metadata.put(key, value);
    }
    
    @SuppressWarnings("unchecked")
    public <T> T getMeta(String key, Class<T> clazz) {
        Object obj = metadata.get(key);
        if (obj == null) return null;
        return (T)obj;
    }
}
