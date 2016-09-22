package org.springframework.data.aerospike.repository;

import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.aerospike.mapping.Field;
import org.springframework.data.annotation.Id;

import java.time.LocalDateTime;
import java.util.Set;

@Document(collection = "person")
public class AnnotatedPerson {

	@Id
	private String id;

	@Field(value = "firstname")
	private String firstName;

	@Field(value = "lastname")
	private String lastName;

	@Field(value = "gender")
	private Person.Sex sex;

	@SuppressWarnings("unused")
	private Integer age;
	
	@Field(value = "createdAt")
	private LocalDateTime creationDate;

	@Field(value = "ShipAddresses")
	private Set<Address> shippingAddresses;

	@Field(value = "ShipAddresses")
	private Long netWorth;

	public AnnotatedPerson() {
		this(null, null, null);
	}

	/**
	 * @param id
	 * @param firstname
	 * @param lastname
	 */
	public AnnotatedPerson(String id, String firstname, String lastname) {
		this(id, firstname, lastname, null);
	}

	/**
	 * @param id
	 * @param firstname
	 * @param lastname
	 * @param age
	 */
	public AnnotatedPerson(String id, String firstname, String lastname, Integer age) {
		this(id, firstname, lastname, age, Person.Sex.MALE);
	}

	/**
	 * @param id
	 * @param firstname
	 * @param lastname
	 * @param age
	 * @param sex
	 */
	public AnnotatedPerson(String id, String firstname, String lastname, Integer age, Person.Sex sex) {
		super();
		this.id = id;
		this.firstName = firstname;
		this.lastName = lastname;
		this.age = age;
		this.sex = sex;
		this.creationDate = LocalDateTime.now();
	}

}
