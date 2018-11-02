/**
 * 
 */
package org.springframework.data.aerospike.repository.query;

import org.springframework.beans.BeanUtils;
import org.springframework.data.aerospike.core.AerospikeOperations;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.repository.query.*;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.spel.standard.SpelExpression;
import org.springframework.util.ClassUtils;

import java.lang.reflect.Constructor;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
public class AerospikePartTreeQuery implements RepositoryQuery {
	
	private final QueryMethodEvaluationContextProvider evaluationContextProvider;
	private final QueryMethod queryMethod;
	private final AerospikeOperations aerospikeOperations;
	private final Class<? extends AbstractQueryCreator<?, ?>> queryCreator;

	public AerospikePartTreeQuery(QueryMethod queryMethod, QueryMethodEvaluationContextProvider evalContextProvider,
			AerospikeOperations aerospikeOperations, Class<? extends AbstractQueryCreator<?, ?>> queryCreator) {

		this.queryMethod = queryMethod;
		this.aerospikeOperations = aerospikeOperations;
		this.evaluationContextProvider = evalContextProvider;
		this.queryCreator = queryCreator;
	}
	
	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.RepositoryQuery#getQueryMethod()
	 */
	@Override
	public QueryMethod getQueryMethod() {
		return queryMethod;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.RepositoryQuery#execute(java.lang.Object[])
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Object execute(Object[] parameters) {
		ParametersParameterAccessor accessor = new ParametersParameterAccessor(queryMethod.getParameters(), parameters);
		Query query = prepareQuery(parameters, accessor);

		if (queryMethod.isPageQuery() || queryMethod.isSliceQuery()) {

			Stream<?> result = findByQuery(query);
			long total = queryMethod.isSliceQuery() ? 0 : aerospikeOperations.count(query, queryMethod.getEntityInformation().getJavaType());

			//TODO: should return SliceImpl for slice query
			return new PageImpl(result.collect(Collectors.toList()), accessor.getPageable(), total);
		} else if (queryMethod.isStreamQuery()) {
			return findByQuery(query);
		} else if (queryMethod.isCollectionQuery()) {
			return findByQuery(query).collect(Collectors.toList());
		} else if (queryMethod.isQueryForEntity()) {
			Stream<?> result = findByQuery(query);
			return result.findFirst().orElse(null);
		}

		throw new UnsupportedOperationException("Query method " + queryMethod.getNamedQueryName() + " not supported.");
	}

	private Stream<?> findByQuery(Query query) {
		return this.aerospikeOperations.find(query, queryMethod.getEntityInformation().getJavaType());
	}

	private Query prepareQuery(Object[] parameters, ParametersParameterAccessor accessor) {
		Query query = createQuery(accessor);

		AerospikeCriteria criteria = (AerospikeCriteria) query.getCriteria();
		Query q = new Query(criteria);

		if (accessor.getPageable().isPaged()) {
			q.setOffset(accessor.getPageable().getOffset());
			q.setRows(accessor.getPageable().getPageSize());
		} else {
			q.setOffset(-1);
			q.setRows(-1);
		}

		if (accessor.getSort().isSorted()) {
			q.setSort(accessor.getSort());
		} else {
			q.setSort(query.getSort());
		}

		if (q.getCriteria() instanceof SpelExpression) {
			EvaluationContext context = this.evaluationContextProvider.getEvaluationContext(queryMethod.getParameters(),
					parameters);
			((SpelExpression) q.getCriteria()).setEvaluationContext(context);
		}

		return q;
	}


	public Query createQuery(ParametersParameterAccessor accessor) {

		PartTree tree = new PartTree(queryMethod.getName(), queryMethod.getEntityInformation().getJavaType());

		Constructor<? extends AbstractQueryCreator<?, ?>> constructor = ClassUtils
				.getConstructorIfAvailable(queryCreator, PartTree.class, ParameterAccessor.class);
		return (Query) BeanUtils.instantiateClass(constructor, tree, accessor).createQuery();
	}

}
