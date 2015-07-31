/**
 * 
 */
package org.springframework.data.aerospike.mapping;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
public class PersonPojoStringId {

	private String id;
	private String text;

	public PersonPojoStringId(String id, String text) {
		this.id = id;
		this.text = text;
	}

	public String getId() {
		return id;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}
}
