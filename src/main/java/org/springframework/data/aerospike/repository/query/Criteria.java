/**
 * 
 */
package org.springframework.data.aerospike.repository.query;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.aerospike.InvalidAerospikeDataAccessApiUsageException;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikePersistentEntity;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.CollectionUtils;

import com.aerospike.client.query.Filter;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
public class Criteria implements CriteriaDefinition {
	
	/**
	 * 
	 */
	private static final String CRITERIA_END = "end";
	/**
	 * 
	 */
	private static final String CRITERIA_BEGIN = "begin";
	/**
	 * 
	 */
	private static final String CRITERIA_EQUAL = "equal";
	/**
	 * Custom "not-null" object as we have to be able to work with {@literal null} values as well.
	 */
	private static final Object NOT_SET = new Object();
	DefaultConversionService cs = new DefaultConversionService();

	private String key;
	private List<Criteria> criteriaChain;
	private LinkedHashMap<String, Object> criteria = new LinkedHashMap<String, Object>();
	private Object isValue = NOT_SET;

	public Criteria(String key) {
		this.criteriaChain = new ArrayList<Criteria>();
		this.criteriaChain.add(this);
		this.key = key;
	}

	/**
	 * 
	 */
	public Criteria() {
		this.criteriaChain = new ArrayList<Criteria>();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.aerospike.repository.query.CriteriaDefinition#getCriteriaObject()
	 */
	@Override
	public Filter getCriteriaObject() {
		
		if (this.criteriaChain.size() == 1) {
			return criteriaChain.get(0).getSingleCriteriaObject();
		} else if (CollectionUtils.isEmpty(this.criteriaChain) && !CollectionUtils.isEmpty(this.criteria)) {
			return getSingleCriteriaObject();
		} else {
			throw new InvalidAerospikeDataAccessApiUsageException(
					"Multiple criteria is not currently supported");
		}
	
	}
	
	protected Filter getSingleCriteriaObject(){
		
		Object equalValue = null;
		Long beginValue = null;
		Long endValue = null;
		Filter filter = null;
		
		for (String k : this.criteria.keySet()) {
			Object value = this.criteria.get(k);
			if(Criteria.CRITERIA_EQUAL.compareTo(k)==0){
				equalValue = value;				
			} else if (Criteria.CRITERIA_BEGIN.compareTo(k)==0) {
				beginValue = (Long) value;
			} else if (Criteria.CRITERIA_END.compareTo(k)==0) {
				endValue = (Long) value;
			}
		}
		
		if(equalValue==null&&beginValue==null&&endValue==null){
			throw new InvalidAerospikeDataAccessApiUsageException(
					"Invalid query: no recognizable criterias");
		} else if ((beginValue==null&&endValue!=null)||(beginValue!=null&&endValue==null)) {
			throw new InvalidAerospikeDataAccessApiUsageException(
					"Invalid query: missing end or start criteria");	
		} else if (beginValue!=null&&endValue!=null) {
			filter = Filter.range(getKey(), beginValue, endValue);
		} else if(equalValue!=null) {
			if(equalValue instanceof Number){
				Long equalLong = (Long) cs.convert(equalValue, TypeDescriptor.valueOf(equalValue.getClass()), TypeDescriptor.valueOf(Long.class));
				filter = Filter.equal(getKey(), equalLong);
			} else {
				String equalString = (String) cs.convert(equalValue, TypeDescriptor.valueOf(equalValue.getClass()), TypeDescriptor.valueOf(String.class));
				filter = Filter.equal(getKey(), equalString);
			}
		}

		return filter;
		
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.aerospike.repository.query.CriteriaDefinition#getKey()
	 */
	@Override
	public String getKey() {
		// TODO Auto-generated method stub
		return this.key;
	}
	/**
	 * Static factory method to create a Criteria using the provided key
	 * 
	 * @param key
	 * @return
	 */
	public static Criteria where(String key) {
		return new Criteria(key);
	}

	/**
	 * @param property 
	 * @param next
	 * @return
	 */
	public Criteria gt(Object o) {
		if (lastOperatorWasNotEqual()) {
			throw new InvalidAerospikeDataAccessApiUsageException("Invalid query: cannot combine range with is");
		}
		Long value = (Long) cs.convert(o, TypeDescriptor.valueOf(o.getClass()), TypeDescriptor.valueOf(Long.class));
		criteria.put(Criteria.CRITERIA_BEGIN, value);
		return this;
	}

	/**
	 * @param next
	 * @return
	 */
	public Criteria gte(Object next) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @param next
	 * @return
	 */
	public Criteria lt(Object o) {
		if (lastOperatorWasNotEqual()) {
			throw new InvalidAerospikeDataAccessApiUsageException("Invalid query: cannot combine range with is");
		}
		Long value = (Long) cs.convert(o, TypeDescriptor.valueOf(o.getClass()), TypeDescriptor.valueOf(Long.class));
		criteria.put(Criteria.CRITERIA_END, value);
		return this;
	}

	/**
	 * @param next
	 * @return
	 */
	public Criteria lte(Object next) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @param object
	 * @return
	 */
	public Criteria ne(Object object) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Creates a criterion using equality
	 * 
	 * @param oParameterAccessor
	 * @return
	 */
	public Criteria is(Object o) {
		if (!isValue.equals(NOT_SET)) {
			throw new InvalidAerospikeDataAccessApiUsageException(
					"Multiple 'is' values declared. You need to use 'and' with multiple criteria");
		}

		if (lastOperatorWasNot()) {
			throw new InvalidAerospikeDataAccessApiUsageException("Invalid query: 'not' can't be used with 'is' - use 'ne' instead.");
		}
		
		if (lastOperatorWasNotRange()) {
			throw new InvalidAerospikeDataAccessApiUsageException("Invalid query: cannot combine range with is");
		}
		this.isValue = o;
		criteria.put(Criteria.CRITERIA_EQUAL, o);
		return this;

	}


	
	
	/**
	 * @return
	 */
	private boolean lastOperatorWasNot() {
		return this.criteria.size() > 0 && "$not".equals(this.criteria.keySet().toArray()[this.criteria.size() - 1]);
	}
	
	/**
	 * @return
	 */
	private boolean lastOperatorWasNotEqual() {
		return this.criteria.size() > 0 && CRITERIA_EQUAL.equals(this.criteria.keySet().toArray()[this.criteria.size() - 1]);
	}
	
	/**
	 * @return
	 */
	private boolean lastOperatorWasNotRange() {
		return this.criteria.size() > 0 
				&& CRITERIA_BEGIN.equals(this.criteria.keySet().toArray()[this.criteria.size() - 1])
				&& CRITERIA_END.equals(this.criteria.keySet().toArray()[this.criteria.size() - 1]);
	}

	/**
	 * @param nextAsArray
	 * @return
	 */
	public Criteria nin(Object nextAsArray) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @param next
	 * @return
	 */
	public Criteria in(Object next) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @param base
	 * @param criteria2
	 * @return
	 */
	public Criteria orOperator(Criteria... criteria) {
		// TODO Auto-generated method stub
		return null;
	}
}
