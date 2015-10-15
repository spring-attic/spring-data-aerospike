package com.aerospike.helper.query;

import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.ClientPolicy;
@RunWith(Parameterized.class)
public class HelperTest {
	protected AerospikeClient client;
	protected ClientPolicy clientPolicy;
	protected QueryEngine queryEngine;
	protected int[] ages = new int[]{25,26,27,28,29};
	protected String[] colours = new String[]{"blue","red","yellow","green","orange"};
	protected String[] animals = new String[]{"cat","dog","mouse","snake","lion"};
	protected boolean useAuth;

	public HelperTest(boolean useAuth){
		this.useAuth = useAuth;
	}
	@Before
	public void setUp() throws Exception {
		if (this.useAuth){
			clientPolicy = new ClientPolicy();
			clientPolicy.failIfNotConnected = true;
			clientPolicy.user = QueryEngineTests.AUTH_UID;
			clientPolicy.password = QueryEngineTests.AUTH_PWD;
			client = new AerospikeClient(clientPolicy, QueryEngineTests.AUTH_HOST, QueryEngineTests.AUTH_PORT);
		} else {
			client = new AerospikeClient(clientPolicy, QueryEngineTests.HOST, QueryEngineTests.PORT);
		}
		queryEngine = new QueryEngine(client);
		int i = 0;
		for (int x = 1; x <= QueryEngineTests.RECORD_COUNT; x++){
			Key key = new Key(QueryEngineTests.NAMESPACE, QueryEngineTests.SET_NAME, "selector-test:"+ x);
			Bin name = new Bin("name", "name:" + x);
			Bin age = new Bin("age", ages[i]);
			Bin colour = new Bin("color", colours[i]);
			Bin animal = new Bin("animal", animals[i]);
			this.client.put(null, key, name, age, colour, animal);
			i++;
			if ( i == 5)
				i = 0;
		}
	}

	@After
	public void tearDown() throws Exception {
		for (int x = 1; x <= QueryEngineTests.RECORD_COUNT; x++){
			Key key = new Key(QueryEngineTests.NAMESPACE, QueryEngineTests.SET_NAME, "selector-test:"+ x);
			this.client.delete(null, key);
		}
		queryEngine.close();
	}

	@Parameterized.Parameters
	   public static Collection connectionStates() {
	      return Arrays.asList(new Object[] {
	         false,
	         true
	      });
	   }


}
