package org.apache.solr.handler.component.aggregates;

import java.util.ArrayList;
import java.util.List;

import org.apache.solr.common.util.NamedList;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.ICardinality;

public class HyperLogLogUnion {
	ICardinality root = null;
	private List<HyperLogLogUnion> children = new ArrayList<HyperLogLogUnion>();
	NamedList<Object> node = null;
	
	public HyperLogLogUnion(NamedList<Object> node) {
		this.node = node;
	}
	
	public void collect() throws CardinalityMergeException {
		for (HyperLogLogUnion a : children) {
			for (HyperLogLogUnion b : children) {
				if (a != b) {
					merge(a, b);
				}
			}
			a.collect();
		}
	}
	
	@SuppressWarnings("unchecked")
	private void merge(HyperLogLogUnion itemA, HyperLogLogUnion itemB) throws CardinalityMergeException {
		for (HyperLogLogUnion a : itemA.children) {
			for (HyperLogLogUnion b : itemB.children) {
				if (a != b) {
					merge(a, b);
				}
			}
		}
		for (HyperLogLogUnion a : itemA.children) {
			for (HyperLogLogUnion b : itemA.children) {
				if (a != b){
					merge(a, b);
				}
			}
		}
		for (HyperLogLogUnion a : itemB.children) {
			for (HyperLogLogUnion b : itemB.children) {
				if (a != b){
					merge(a, b);
				}
			}
		}
		
		if (false == itemB.node instanceof ExtraNamedList) return;
		ExtraNamedList itemBMeta = (ExtraNamedList)itemB.node;
        String path = itemBMeta.getMeta("path", String.class);  
        String[] paths = path.split("/");
		
		ICardinality uniques = itemA.root.merge(itemB.root);
		long totalA = itemA.root.cardinality();
		long totalB = itemB.root.cardinality();
		long union = uniques.cardinality();
		long intersection = (totalA + totalB) - union;
		
		NamedList<Object> wrap = new NamedList<Object>();
		wrap.add("intersect", intersection);
		wrap.add("union", union);
		wrap.add("total", totalA + totalB);
		if (itemA.node.get("join") == null) {
			itemA.node.add("join", new NamedList<Object>());
		}
		NamedList<Object> items = ((NamedList<Object>) itemA.node.get("join"));
		
		for (int i=0;i<paths.length;i++) {
		    String rel = paths[i];
            String value = rel.split(":")[1];
            if (rel.matches("^.*:\\[.*\\sTO\\s.*\\]$")) {
                value = rel.substring(rel.indexOf(":")+1).substring(1, rel.indexOf(" TO ")-3);
            }
            if (items.get(value) == null) {
                NamedList<Object> n = (i == paths.length-1) ? wrap : new NamedList<Object>();
                items.add(value, n);
                items = n;
            } else {
                items = (NamedList<Object>)items.get(value);
            }
		}
	}
	
	public void add(HyperLogLogUnion union) {
		children.add(union);
		if (root == null) {
			root = union.root;
		} else {
			try {
				root = this.root.merge(union.root);
			} catch (CardinalityMergeException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
