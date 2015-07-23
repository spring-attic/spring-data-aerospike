/**
 * 
 */
package org.springframework.data.aerospike.repository.query;

import org.springframework.data.repository.query.ParameterAccessor;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
public interface AerospikeParameterAccessor extends ParameterAccessor {
	/**
	 * Returns the {@link TextCriteria} to be used for full text query.
	 * 
	 * @return null if not set.
	 * @since 1.6
	 */
	TextCriteria getFullText();

	/**
	 * Returns the raw parameter values of the underlying query method.
	 * 
	 * @return
	 * @since 1.8
	 */
	Object[] getValues();
}
