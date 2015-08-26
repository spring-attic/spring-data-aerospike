/**
 * 
 */
package org.springframework.data.aerospike.example;

import java.util.List;

import org.springframework.data.aerospike.example.data.Person;
import org.springframework.data.aerospike.repository.AerospikeRepository;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
public interface PersonRepository extends AerospikeRepository<Person, String> {

	List<Person> findByName(String name);

	List<Person> findByNameStartsWith(String prefix);

}
