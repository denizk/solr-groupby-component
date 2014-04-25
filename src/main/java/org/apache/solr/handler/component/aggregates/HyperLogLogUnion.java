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
		
		if (itemB.node.get("value") == null) return;
		String key = itemB.node.get("value").toString();
		String path = itemB.node.get("path") != null ? itemB.node.get("path").toString() : "";
		if (path.length() > 0 && path.indexOf("/") >= 0) {
			path = path.substring(0, path.indexOf("/"));
			path = path.substring(path.indexOf(":")+1);
		} else {
			path = key;
		}
		
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
		if (key == path) {
			items.add(key, wrap);
		} else {
			if (items.get(path) == null) {
				items.add(path, new NamedList<Object>());
			}
			((NamedList<Object>)items.get(path)).add(key, wrap);
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
