/**
 * 
 */
package org.springframework.data.aerospike.core;

import java.util.UUID;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
public class Person {
	private final String id;

	private String firstName;

	private int age;

	private Person friend;

	private boolean active = true;

	public Person() {
		this.id = UUID.randomUUID().toString();
	}

	@Override
	public String toString() {
		return "Person [id=" + id + ", firstName=" + firstName + ", age=" + age + ", friend=" + friend + "]";
	}

	public Person(String id, String firstname) {
		this.id = id;
		this.firstName = firstname;
	}

	public Person(String firstname, int age) {
		this();
		this.firstName = firstname;
		this.age = age;
	}

	public Person(String firstname) {
		this();
		this.firstName = firstname;
	}

	public String getId() {
		return id;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public Person getFriend() {
		return friend;
	}

	public void setFriend(Person friend) {
		this.friend = friend;
	}

	/**
	 * @return the active
	 */
	public boolean isActive() {
		return active;
	}

	/*
	  * (non-Javadoc)
	  *
	  * @see java.lang.Object#equals(java.lang.Object)
	  */
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}

		if (obj == null) {
			return false;
		}

		if (!(getClass().equals(obj.getClass()))) {
			return false;
		}

		Person that = (Person) obj;

		return this.id == null ? false : this.id.equals(that.id);
	}

	/* (non-Javadoc)
	  * @see java.lang.Object#hashCode()
	  */
	@Override
	public int hashCode() {
		return id.hashCode();
	}
}
