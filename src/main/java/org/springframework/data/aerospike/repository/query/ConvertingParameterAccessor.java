/**
 * 
 */
package org.springframework.data.aerospike.repository.query;

import java.util.Iterator;

import org.springframework.data.aerospike.core.AerospikeWriter;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.util.Assert;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
public class ConvertingParameterAccessor implements AerospikeParameterAccessor {
	
	@SuppressWarnings("unused")
	private final AerospikeWriter<?> writer;
	private final AerospikeParameterAccessor delegate;
	
	public ConvertingParameterAccessor(AerospikeWriter<?> writer, AerospikeParameterAccessor delegate) {

		Assert.notNull(writer);
		Assert.notNull(delegate);

		this.writer = writer;
		this.delegate = delegate;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParameterAccessor#getPageable()
	 */
	@Override
	public Pageable getPageable() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParameterAccessor#getSort()
	 */
	@Override
	public Sort getSort() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParameterAccessor#getBindableValue(int)
	 */
	@Override
	public Object getBindableValue(int index) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParameterAccessor#hasBindableNullValue()
	 */
	@Override
	public boolean hasBindableNullValue() {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParameterAccessor#iterator()
	 */
	@Override
	public Iterator<Object> iterator() {
		return delegate.iterator();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.aerospike.repository.query.AerospikeParameterAccessor#getFullText()
	 */
	@Override
	public TextCriteria getFullText() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.aerospike.repository.query.AerospikeParameterAccessor#getValues()
	 */
	@Override
	public Object[] getValues() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Class<?> getDynamicProjection() {
		// TODO Auto-generated method stub
		return null;
	}

}
