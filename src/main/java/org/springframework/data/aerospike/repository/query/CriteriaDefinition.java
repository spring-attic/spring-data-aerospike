/**
 * 
 */
package org.springframework.data.aerospike.repository.query;

import java.util.List;

import com.aerospike.client.query.Filter;
import com.aerospike.helper.query.Qualifier;

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
	List<Qualifier> getCriteriaObject();
	/**
	 * Get the identifying {@literal key}.
	 * 
	 * @return
	 * @since 1.6
	 */
	String getKey();
}
