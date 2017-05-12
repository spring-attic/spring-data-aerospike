/**
 *
 */
package org.springframework.data.aerospike.convert;

import com.aerospike.client.Key;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;

public class AerospikeReadDataTest {

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Test
	public void shouldThrowExceptionIfRecordIsNull() throws Exception {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("Record should not be null");

		AerospikeReadData.forRead(new Key("namespace", "set", 867), null);
	}

	@Test
	public void shouldThrowExceptionIfKeyIsNull() throws Exception {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("Key should not be null");

		AerospikeReadData.forRead(null, new HashMap<>());
	}
}
