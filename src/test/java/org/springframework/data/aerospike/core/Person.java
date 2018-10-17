/**
 * 
 */
package org.springframework.data.aerospike.core;

import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

import lombok.*;
import org.springframework.data.aerospike.mapping.Field;
import org.springframework.data.annotation.Id;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
@AllArgsConstructor
@Getter
@Setter
@EqualsAndHashCode
@ToString
@Builder
public class Person {
	private @Id String id;
	private String firstName;
	private int age;
	private Map<String, String> map;
	private Person friend;
	private boolean active;
	private Date dateOfBirth;
	private ArrayList<String> list;

	@Field("email") 
	private String emailAddress;

	public Person() {
		super();
	}

	public Person(String id, String firstname) {
		this.id = id;
		this.firstName = firstname;
	}

	public Person(String id, String firstname, int age) {
		this.id = id;
		this.firstName = firstname;
		this.age = age;
	}

}
