/**
 * 
 */
package org.springframework.data.aerospike.repository.query;

import com.aerospike.client.query.Filter;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
public interface CriteriaDefinition {
	/**
	 * Get {@link Filter} representation.
	 * 
	 * @return
	 */
	Filter getCriteriaObject();
	/**
	 * Get the identifying {@literal key}.
	 * 
	 * @return
	 * @since 1.6
	 */
	String getKey();
}
