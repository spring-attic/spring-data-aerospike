package org.springframework.data.aerospike.core.aggregation;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.springframework.util.Assert;

import com.aerospike.client.query.ResultSet;

public class AggregationResults<T> {
	private final List<T> mappedResults;
	private final ResultSet rawResults;
	public AggregationResults(List<T> mappedResults, ResultSet rawResults) {

		Assert.notNull(mappedResults);
		Assert.notNull(rawResults);
		this.rawResults = rawResults;
		this.mappedResults = Collections.unmodifiableList(mappedResults);
	}
	/**
	 * Returns the aggregation results.
	 * 
	 * @return
	 */
	public List<T> getMappedResults() {
		return mappedResults;
	}
	/**
	 * Returns the unique mapped result. Assumes no result or exactly one.
	 * 
	 * @return
	 * @throws IllegalArgumentException in case more than one result is available.
	 */
	public T getUniqueMappedResult() {
		Assert.isTrue(mappedResults.size() < 2, "Expected unique result or null, but got more than one!");
		return mappedResults.size() == 1 ? mappedResults.get(0) : null;
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	public Iterator<T> iterator() {
		return mappedResults.iterator();
	}
	/**
	 * Returns the raw ResultSet that was returned by the server.
	 * 
	 * @return
	 */
	public ResultSet getRawResults() {
		return rawResults;
	}

}
