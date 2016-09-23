/**
 * 
 */
package org.springframework.data.aerospike.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.example.data.Person;
import org.springframework.data.aerospike.repository.query.Criteria;
import org.springframework.data.aerospike.repository.query.Query;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.query.IndexType;
/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
public class AerospikeApp {

	private static final Logger LOG = LoggerFactory
			.getLogger(AerospikeApp.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		try {
			String localhost = "127.0.0.1"; // make sure you change this to the correct address
			AerospikeClient client = new AerospikeClient(null, localhost, 3000);
			AerospikeTemplate aerospikeTemplate = new AerospikeTemplate(client,
					"test");
			aerospikeTemplate.createIndex(Person.class,
					"Person_firstName_index", "name", IndexType.STRING);
			Person personSven01 = new Person("Sven-01", "ZName", 25);
			Person personSven02 = new Person("Sven-02", "QName", 21);
			Person personSven03 = new Person("Sven-03", "AName", 24);
			Person personSven04 = new Person("Sven-04", "WName", 25);

			aerospikeTemplate.delete(Person.class);

			aerospikeTemplate.insert(personSven01);
			aerospikeTemplate.insert(personSven02);
			aerospikeTemplate.insert(personSven03);
			aerospikeTemplate.insert(personSven04);

			Query<?> query = new Query<Object>(
					Criteria.where("Person").is("WName", "name"));

			Iterable<Person> it = aerospikeTemplate.find(query, Person.class);

			Person firstPerson = null;
			for (Person person : it) {
				firstPerson = person;
				LOG.info(firstPerson.toString());
				System.out.println(firstPerson.toString());
			}
		}
		catch (AerospikeException e) {
			e.printStackTrace();
		}

	}

}
