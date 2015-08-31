/**
 * 
 */
package org.springframework.data.aerospike.repository;

import java.util.Collection;
import java.util.Date;
import java.util.List;
//import java.util.stream.Stream;

import org.springframework.data.aerospike.repository.Person.Sex;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
public interface PersonRepository extends AerospikeRepository<Person, String> {

	List<Person> findByLastname(String lastname);
	
	List<Person> findByLastnameStartsWith(String prefix);

	List<Person> findByLastnameEndsWith(String postfix);

	List<Person> findByLastnameOrderByFirstnameAsc(String lastname);
	
	List<Person> findByLastnameOrderByFirstnameDesc(String lastname);

	List<Person> findByFirstnameLike(String firstname);

	List<Person> findByFirstnameLikeOrderByLastnameAsc(String firstname, Sort sort);

	List<Person> findByAgeLessThan(int age, Sort sort);

	List<Person> findByFirstnameIn(String... firstnames);

	List<Person> findByFirstnameNotIn(Collection<String> firstnames);

	List<Person> findByFirstnameAndLastname(String firstname, String lastname);

	List<Person> findByAgeBetween(int from, int to);

	Person findByShippingAddresses(Address address);

	List<Person> findByAddress(Address address);

	List<Person> findByAddressZipCode(String zipCode);

	List<Person> findByLastnameLikeAndAgeBetween(String lastname, int from, int to);

	List<Person> findByAgeOrLastnameLikeAndFirstnameLike(int age, String lastname, String firstname);

	List<Person> findBySex(Sex sex);

	List<Person> findBySex(Sex sex, Pageable pageable);

//	List<Person> findByNamedQuery(String firstname);

	List<Person> findByCreator(User user);

	List<Person> findByCreatedAtLessThan(Date date);

	List<Person> findByCreatedAtGreaterThan(Date date);

//	List<Person> findByCreatedAtLessThanManually(Date date);

	List<Person> findByCreatedAtBefore(Date date);

	List<Person> findByCreatedAtAfter(Date date);

	List<Person> findByLastnameNot(String lastname);

	List<Person> findByCredentials(Credentials credentials);
	
	List<Person> findCustomerByAgeBetween(Integer from, Integer to);
	
	List<Person> findPersonByFirstname(String firstname);

	long countByLastname(String lastname);

	int countByFirstname(String firstname);

	long someCountQuery(String lastname);

	List<Person> findByFirstnameIgnoreCase(String firstName);

	List<Person> findByFirstnameNotIgnoreCase(String firstName);

	List<Person> findByFirstnameStartingWithIgnoreCase(String firstName);

	List<Person> findByFirstnameEndingWithIgnoreCase(String firstName);

	List<Person> findByFirstnameContainingIgnoreCase(String firstName);

	Slice<Person> findByAgeGreaterThan(int age, Pageable pageable);

//	Page<Person> findByHavingCreator(Pageable page);

	List<Person> deleteByLastname(String lastname);

	Long deletePersonByLastname(String lastname);

//	List<Person> removeByLastnameUsingAnnotatedQuery(String lastname);

//	Long removePersonByLastnameUsingAnnotatedQuery(String lastname);

	Page<Person> findByAddressIn(List<Address> address, Pageable page);

//	@Query("{firstname:{$in:?0}, lastname:?1}")
//	Page<Person> findByCustomQueryFirstnamesAndLastname(List<String> firstnames, String lastname, Pageable page);

//	@Query("{lastname:?0, address.street:{$in:?1}}")
//	Page<Person> findByCustomQueryLastnameAndAddressStreetInList(String lastname, List<String> streetNames, Pageable page);

	List<Person> findTop3ByLastnameStartingWith(String lastname);

	Page<Person> findTop3ByLastnameStartingWith(String lastname, Pageable pageRequest);

//	List<Person> findByKeyValue(String key, String value);
//
//	Stream<Person> findByCustomQueryWithStreamingCursorByFirstnames(List<String> firstnames);
//
//	List<Person> findWithSpelByFirstnameForSpELExpressionWithParameterIndexOnly(String firstname);
//	
//	List<Person> findWithSpelByFirstnameAndCurrentUserWithCustomQuery(String firstname);

	/**
	 * @param string
	 * @return
	 */
	List<Person> findByFirstname(String string);


	/**
	 * @param string
	 * @param i
	 * @return
	 */
	List<Person> findByFirstnameAndAge(String string, int i);

	/**
	 * @param i
	 * @param j
	 * @param string
	 * @return
	 */
	Iterable<Person> findByAgeBetweenAndLastname(int from, int to, String lastname);

	/**
	 * @param string
	 * @return
	 */
	List<Person> findByFirstnameStartsWith(String string);

	/**
	 * @param i
	 * @param j
	 * @return
	 */
	Iterable<Person> findByAgeBetweenOrderByLastname(int i, int j);

	/**
	 * @param string
	 * @param pageRequest
	 * @return
	 */
//	Page<Person> findByLastnameLike(String string, PageRequest pageRequest);
	
//	@Query("{ firstname : :#{#firstname}}")
//	List<Person> findWithSpelByFirstnameForSpELExpressionWithParameterVariableOnly(@Param("firstname") String firstname);
}
