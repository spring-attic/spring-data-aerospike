/**
 * Copyright (c) 2015 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 *
 */
package org.springframework.data.aerospike.repository;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.springframework.data.aerospike.mapping.Field;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
public class Person extends Contact implements Comparable<Person> {

	public enum Sex {
		MALE, FEMALE;
	}

	private String firstname;
	private HashMap<?,?> myHashMap;
	private String lastname;
	private String email;
	private Integer age;
	private Sex sex;

	private LocalDateTime createdAt;

	List<String> skills;
	//@DBRef(lazy = true)
	private Address address;
	@Field(value = "ShipAddresses")
	private Set<Address> shippingAddresses;

	User creator;

	//User coworker;

	//List<User> fans;

	//ArrayList<User> realFans;

	Credentials credentials;

	public Person() {
		this(null, null, null);
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
	public Person(String id, String firstname, String lastname, Integer age) {

		this(id, firstname, lastname, age, Sex.MALE);
	}

	/**
	 * @param id
	 * @param firstname
	 * @param lastname
	 * @param age
	 * @param sex
	 */
	public Person(String id, String firstname, String lastname, Integer age, Sex sex) {
		super();
		this.id = id;
		this.firstname = firstname;
		this.lastname = lastname;
		this.age = age;
		this.sex = sex;
		this.email = (firstname == null ? "none" : firstname.toLowerCase()) + "@dmband.com";
		this.createdAt = LocalDateTime.now();
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((address == null) ? 0 : address.hashCode());
		result = prime * result + ((age == null) ? 0 : age.hashCode());
		result = prime * result + ((creator == null) ? 0 : creator.hashCode());
		result = prime * result
				+ ((credentials == null) ? 0 : credentials.hashCode());
		result = prime * result + ((email == null) ? 0 : email.hashCode());
		result = prime * result
				+ ((firstname == null) ? 0 : firstname.hashCode());
		result = prime * result
				+ ((lastname == null) ? 0 : lastname.hashCode());
		result = prime * result + ((sex == null) ? 0 : sex.hashCode());
		result = prime * result + ((shippingAddresses == null) ? 0
				: shippingAddresses.hashCode());
		result = prime * result + ((skills == null) ? 0 : skills.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		Person other = (Person) obj;
		if (address == null) {
			if (other.address != null)
				return false;
		} else if (!address.equals(other.address))
			return false;
		if (age == null) {
			if (other.age != null)
				return false;
		} else if (!age.equals(other.age))
			return false;
		if (creator == null) {
			if (other.creator != null)
				return false;
		} else if (!creator.equals(other.creator))
			return false;
		if (credentials == null) {
			if (other.credentials != null)
				return false;
		} else if (!credentials.equals(other.credentials))
			return false;
		if (email == null) {
			if (other.email != null)
				return false;
		} else if (!email.equals(other.email))
			return false;
		if (firstname == null) {
			if (other.firstname != null)
				return false;
		} else if (!firstname.equals(other.firstname))
			return false;
		if (lastname == null) {
			if (other.lastname != null)
				return false;
		} else if (!lastname.equals(other.lastname))
			return false;
		if (sex != other.sex)
			return false;
		if (shippingAddresses == null) {
			if (other.shippingAddresses != null)
				return false;
		} else if (!shippingAddresses.equals(other.shippingAddresses))
			return false;
		if (skills == null) {
			if (other.skills != null)
				return false;
		} else if (!skills.equals(other.skills))
			return false;
		return true;
	}


	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(Person another) {
		return this.lastname.compareTo(another.lastname);
	}

	public String getFirstname() {
		return firstname;
	}

	public void setFirstname(String firstname) {
		this.firstname = firstname;
	}

	public String getLastname() {
		return lastname;
	}

	public void setLastname(String lastname) {
		this.lastname = lastname;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public Integer getAge() {
		return age;
	}

	public void setAge(Integer age) {
		this.age = age;
	}

	public Sex getSex() {
		return sex;
	}

	public void setSex(Sex sex) {
		this.sex = sex;
	}

	public LocalDateTime getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(LocalDateTime createdAt) {
		this.createdAt = createdAt;
	}

	public List<String> getSkills() {
		return skills;
	}

	public void setSkills(List<String> skills) {
		this.skills = skills;
	}

	public Address getAddress() {
		return address;
	}

	public void setAddress(Address address) {
		this.address = address;
	}

	public Set<Address> getShippingAddresses() {
		return shippingAddresses;
	}

	public void setShippingAddresses(Set<Address> shippingAddresses) {
		this.shippingAddresses = shippingAddresses;
	}

	public User getCreator() {
		return creator;
	}

	public void setCreator(User creator) {
		this.creator = creator;
	}


//	public User getCoworker() {
//		return coworker;
//	}
//
//
//	public void setCoworker(User coworker) {
//		this.coworker = coworker;
//	}


//	public List<User> getFans() {
//		return fans;
//	}
//
//
//	public void setFans(List<User> fans) {
//		this.fans = fans;
//	}
//
//
//	public ArrayList<User> getRealFans() {
//		return realFans;
//	}
//
//
//	public void setRealFans(ArrayList<User> realFans) {
//		this.realFans = realFans;
//	}
//
//
//	public Credentials getCredentials() {
//		return credentials;
//	}
//
//
//	public void setCredentials(Credentials credentials) {
//		this.credentials = credentials;
//	}


	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Person [firstname=" + firstname + ", lastname=" + lastname
				+ ", email=" + email + ", age=" + age + ", sex=" + sex
				+ ", createdAt=" + createdAt + ", skills=" + skills
				+ ", address=" + address + ", shippingAddresses="
				+ shippingAddresses + ", creator=" + creator + "]";
	}

	/**
	 * @return the myHashMap
	 */
	public HashMap<?,?> getMyHashMap() {
		return myHashMap;
	}

	/**
	 * @param myHashMap the myHashMap to set
	 */
	public void setMyHashMap(HashMap<?,?> myHashMap) {
		this.myHashMap = myHashMap;
	}

}
