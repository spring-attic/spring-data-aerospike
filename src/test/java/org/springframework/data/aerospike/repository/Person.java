/*******************************************************************************
 * Copyright (c) 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 * 		http://www.apache.org/licenses/LICENSE-2.0
 *  	
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
/**
 * 
 */
package org.springframework.data.aerospike.repository;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.aerospike.mapping.Field;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
@Document
public class Person extends Contact {


	public enum Sex {
		MALE, FEMALE;
	}

	private String firstname;
	private String lastname;
	//@Indexed(unique = true, dropDups = true) 
	private String email;
	private Integer age;
	@SuppressWarnings("unused") private Sex sex;
	Date createdAt;

	List<String> skills;
	//@DBRef(lazy = true)
	private Address address;
	@Field(name="ShipAddresses")
	private Set<Address> shippingAddresses;

	User creator;

	User coworker;

	List<User> fans;

	ArrayList<User> realFans;

	Credentials credentials;
	
	public Person() {

		this(null,null, null);
	}


	/**
	 * @param id
	 * @param firstname
	 * @param lastname
	 */
	public Person(String id, String firstname, String lastname) {

		this(id, firstname, lastname, null);
	}

	/**
	 * @param id
	 * @param firstname
	 * @param lastname
	 * @param age
	 */
	public Person(String id,String firstname, String lastname, Integer age) {

		this(id, firstname, lastname, age, Sex.MALE);
	}

	/**
	 * @param id
	 * @param firstname
	 * @param lastname
	 * @param age
	 * @param sex
	 */
	public Person(String id,String firstname, String lastname, Integer age, Sex sex) { 
		super();
		this.id = id;
		this.firstname = firstname;
		this.lastname = lastname;
		this.age = age;
		this.sex = sex;
		this.email = (firstname == null ? "noone" : firstname.toLowerCase()) + "@dmband.com";
		this.createdAt = new Date();
	}


}
