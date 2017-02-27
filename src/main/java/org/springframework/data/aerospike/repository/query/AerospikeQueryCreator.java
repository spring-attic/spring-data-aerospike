/**
 * 
 */
package org.springframework.data.aerospike.repository.query;

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.mapping.CachingAerospikePersistentProperty;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.context.PersistentPropertyPath;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.Part.IgnoreCaseType;
import org.springframework.data.repository.query.parser.Part.Type;
import org.springframework.data.repository.query.parser.PartTree;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
public class AerospikeQueryCreator extends 	AbstractQueryCreator<Query<?>, Criteria> {

	private static final Logger LOG = LoggerFactory.getLogger(AerospikeQueryCreator.class);
	//private ParameterAccessor accessor;
	private MappingContext<?, AerospikePersistentProperty> context;

	/**
	 * @param tree
	 * @param parameters
	 */
	public AerospikeQueryCreator(PartTree tree, ParameterAccessor parameters) {
		super(tree, parameters);
		this.context = new AerospikeMappingContext();
	}

	/**
	 * @param tree
	 * @param parameters
	 */
	public AerospikeQueryCreator(PartTree tree, ParameterAccessor parameters,MappingContext<?, AerospikePersistentProperty> context) {
		super(tree, parameters);
		this.context = context;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.parser.AbstractQueryCreator#create(org.springframework.data.repository.query.parser.Part, java.util.Iterator)
	 */
	@Override
	protected Criteria create(Part part, Iterator<Object> iterator) {
		PersistentPropertyPath<AerospikePersistentProperty> path = context.getPersistentPropertyPath(part.getProperty());
		AerospikePersistentProperty property = path.getLeafProperty();
		Criteria criteria = from(part, property, Criteria.where(path.toDotPath()), iterator);
		return criteria;
	}

	/**
	 * Populates the given {@link CriteriaDefinition} depending on the {@link Part} given.
	 * 
	 * @param part
	 * @param property
	 * @param criteria
	 * @param parameters
	 * @return
	 */
	private Criteria from(Part part, AerospikePersistentProperty property, Criteria criteria, Iterator<?> parameters) {
		Type type = part.getType();
		String fieldName = ((CachingAerospikePersistentProperty) property).getFieldName();
		IgnoreCaseType ignoreCase = part.shouldIgnoreCase();
		
		switch (type) {
			case AFTER:
			case GREATER_THAN:
				return criteria.gt(parameters.next(), fieldName);
			case GREATER_THAN_EQUAL:
				return criteria.gte(parameters.next(), fieldName);
			case BEFORE:
			case LESS_THAN:
				return criteria.lt(parameters.next(), fieldName);
			case LESS_THAN_EQUAL:
				return criteria.lte(parameters.next(), fieldName);
			case BETWEEN:
				return criteria.between(parameters.next(),parameters.next(), fieldName );
			case IS_NOT_NULL:
				return criteria.ne(null);
			case IS_NULL:
			case NOT_IN:
				return null;
			case IN:
				return criteria.in(parameters.next());
			case LIKE:
			case STARTING_WITH:
				return criteria.startingWith(parameters.next(), fieldName, ignoreCase);
			case ENDING_WITH:
				return null;
			case CONTAINING:
				return criteria.containing(parameters.next(), fieldName, ignoreCase);
			case NOT_CONTAINING:
				return null;
			case REGEX:
				return null;
			case EXISTS:
				return null;
			case TRUE:
			case FALSE:
			case NEAR:
				return null;
			case WITHIN:
				return criteria.geo_within(parameters.next(), parameters.next(), parameters.next(), fieldName);
			case SIMPLE_PROPERTY:
				return criteria.is(parameters.next(), fieldName);
			case NEGATING_SIMPLE_PROPERTY:
				return criteria.ne(parameters.next());
			default:
				throw new IllegalArgumentException("Unsupported keyword!");
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.parser.AbstractQueryCreator#and(org.springframework.data.repository.query.parser.Part, java.lang.Object, java.util.Iterator)
	 */
	@Override
	protected Criteria and(Part part, Criteria base, Iterator<Object> iterator) {
		if (base == null) {
			return create(part, iterator);
		}
		PersistentPropertyPath<AerospikePersistentProperty> path = context.getPersistentPropertyPath(part.getProperty());
		AerospikePersistentProperty property = path.getLeafProperty();
		
		return from(part, property, base.and(path.toDotPath()), iterator);

		//return from(part, property, Criteria.where(path.toDotPath()), iterator);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.parser.AbstractQueryCreator#or(java.lang.Object, java.lang.Object)
	 */
	@Override
	protected Criteria or(Criteria base, Criteria criteria) {
		Criteria result = new Criteria();
		return result.orOperator(base, criteria);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.parser.AbstractQueryCreator#complete(java.lang.Object, org.springframework.data.domain.Sort)
	 */
	@Override
	protected Query<?> complete(Criteria criteria, Sort sort) {
		Query<?> query = (criteria == null ? new Query<Object>() : new Query<Object>(criteria)).with(sort);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Created query " + query);
		}

		return query;
	}

	@SuppressWarnings("unused")
	private boolean isSimpleComparisionPossible(Part part) {
		switch (part.shouldIgnoreCase()) {
			case NEVER:
				return true;
			case WHEN_POSSIBLE:
				return part.getProperty().getType() != String.class;
			case ALWAYS:
				return false;
			default:
				return true;
		}
	}

}
