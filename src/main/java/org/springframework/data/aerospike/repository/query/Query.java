/**
 * 
 */
package org.springframework.data.aerospike.repository.query;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.aerospike.InvalidAerospikeDataAccessApiUsageException;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.keyvalue.core.query.KeyValueQuery;

import com.aerospike.helper.query.Qualifier;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 * @param <T>
 *
 */
public class Query<T> {

	private Sort sort;
	private int offset = -1;
	private int rows = -1;
	private final Map<String, CriteriaDefinition> criteria = new LinkedHashMap<String, CriteriaDefinition>();

	/**
	 * Creates new instance of {@link KeyValueQuery}.
	 */
	public Query() {
	}

	/**
	 * Creates new instance of {@link KeyValueQuery} with given criteria.
	 * 
	 * @param criteria can be {@literal null}.
	 */
	public Query(CriteriaDefinition criteria) {
		addCriteria(criteria);
		// this.criteria = criteria;
	}

	/**
	 * Adds the given {@link CriteriaDefinition} to the current {@link Query}.
	 * 
	 * @param criteriaDefinition must not be {@literal null}.
	 * @return
	 * @since 1.6
	 */
	public Query<?> addCriteria(CriteriaDefinition criteriaDefinition) {

		CriteriaDefinition existing = this.criteria.get(criteriaDefinition.getKey());
		String key = criteriaDefinition.getKey();

		if (existing == null) {
			this.criteria.put(key, criteriaDefinition);
		}
		else {
			throw new InvalidAerospikeDataAccessApiUsageException(
					"Due to limitations of the Filter, "
							+ "you can't add a second '" + key + "' criteria. "
							+ "Query already contains '"
							+ existing.getCriteriaObject() + "'.");
		}

		return this;
	}

	/**
	 * Creates new instance of {@link Query} with given {@link Sort}.
	 * 
	 * @param sort can be {@literal null}.
	 */
	public Query(Sort sort) {
		this.sort = sort;
	}

	/**
	 * Get the criteria object.
	 * 
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public T getCritieria() {
		T value = null;
		for (Map.Entry<String, CriteriaDefinition> entry : this.criteria
				.entrySet()) {
			value = (T) entry.getValue();
			// now work with key and value...
		}
		return value;
	}

	/**
	 * Get {@link Sort}.
	 * 
	 * @return
	 */
	public Sort getSort() {
		return sort;
	}

	/**
	 * Number of elements to skip.
	 * 
	 * @return negative value if not set.
	 */
	public int getOffset() {
		return this.offset;
	}

	/**
	 * Number of elements to read.
	 * 
	 * @return negative value if not set.
	 */
	public int getRows() {
		return this.rows;
	}

	/**
	 * Set the number of elements to skip.
	 * 
	 * @param offset use negative value for none.
	 */
	public void setOffset(int offset) {
		this.offset = offset;
	}

	/**
	 * Set the number of elements to read.
	 * 
	 * @param offset use negative value for all.
	 */
	public void setRows(int rows) {
		this.rows = rows;
	}

	/**
	 * Set {@link Sort} to be applied.
	 * 
	 * @param sort
	 */
	public void setSort(Sort sort) {
		this.sort = sort;
	}

	/**
	 * Add given {@link Sort}.
	 * 
	 * @param sort {@literal null} {@link Sort} will be ignored.
	 * @return
	 */
	public Query<T> orderBy(Sort sort) {
		if (sort == null) {
			return this;
		}

		if (this.sort != null) {
			this.sort.and(sort);
		}
		else {
			this.sort = sort;
		}
		return this;
	}

	/**
	 * @see Query#setOffset(int)
	 * @param offset
	 * @return
	 */
	public Query<T> skip(int offset) {
		setOffset(offset);
		return this;
	}

	/**
	 * @see Query#setRows(int)
	 * @param rows
	 * @return
	 */
	public Query<T> limit(int rows) {
		setRows(rows);
		return this;
	}

	/**
	 * @param sort
	 * @return
	 */
	public Query<?> with(Sort sort) {
		if (sort == null) {
			return this;
		}

		for (Order order : sort) {
			if (order.isIgnoreCase()) {
				throw new IllegalArgumentException(String.format(
						"Given sort contained an Order for %s with ignore case! "
								+ "Aerospike does not support sorting ignoreing case currently!",
						order.getProperty()));
			}
		}

		if (this.sort == null) {
			this.sort = sort;
		}
		else {
			this.sort = this.sort.and(sort);
		}

		return this;
	}

	public List<Qualifier> getQueryObject() {
		List<Qualifier> qualifiers = null;
		for (String k : criteria.keySet()) {
			CriteriaDefinition c = criteria.get(k);
			qualifiers = c.getCriteriaObject();
		}

		return qualifiers;
	}

//	/* (non-Javadoc)
//	 * @see java.lang.Object#toString()
//	 */
//	@Override
//	public String toString() {
//		StringBuilder res = new StringBuilder();
//		for (Qualifier qualifier : getQueryObject()) {
//			res.append(qualifier.luaFilterString());
//			res.append(',');
//		}
//		return res.toString();
//	}

}
