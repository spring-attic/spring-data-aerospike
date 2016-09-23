/**
 * 
 */
package org.springframework.data.aerospike.repository.query;

import java.util.Arrays;
import java.util.Iterator;

import org.springframework.data.aerospike.core.AerospikeWriter;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Sort;
import org.springframework.data.geo.Distance;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
public class StubParameterAccessor implements AerospikeParameterAccessor {
	
	private final Object[] values;
	
	@SuppressWarnings("unused")
	private Range<Distance> range = new Range<Distance>(null, null);
	
	/**
	 * Creates a new {@link ConvertingParameterAccessor} backed by a {@link StubParameterAccessor} simply returning the
	 * given parameters converted but unfiltered.
	 * 
	 * @param converter
	 * @param parameters
	 * @return
	 */
	public static ConvertingParameterAccessor getAccessor(AerospikeWriter<Object> converter, Object... parameters) {
		return new ConvertingParameterAccessor(converter, new StubParameterAccessor(parameters));
		//ParametersParameterAccessor(Parameters<?, ?> parameters, Object[] values) {
	}
	@SuppressWarnings("unchecked")
	public StubParameterAccessor(Object... values) {

		this.values = values;

		for (Object value : values) {
			if (value instanceof Range) {
				this.range = (Range<Distance>) value;
			} else if (value instanceof Distance) {
				this.range = new Range<Distance>(null, (Distance) value);
			}
		}
	}
	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParameterAccessor#getPageable()
	 */
	@Override
	public Pageable getPageable() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParameterAccessor#getSort()
	 */
	@Override
	public Sort getSort() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParameterAccessor#getBindableValue(int)
	 */
	@Override
	public Object getBindableValue(int index) {
		return values[index];
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParameterAccessor#hasBindableNullValue()
	 */
	@Override
	public boolean hasBindableNullValue() {
		return false;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParameterAccessor#iterator()
	 */
	@Override
	public Iterator<Object> iterator() {
		return Arrays.asList(values).iterator();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.aerospike.repository.query.AerospikeParameterAccessor#getFullText()
	 */
	@Override
	public TextCriteria getFullText() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.aerospike.repository.query.AerospikeParameterAccessor#getValues()
	 */
	@Override
	public Object[] getValues() {
		return this.values;
	}

}
