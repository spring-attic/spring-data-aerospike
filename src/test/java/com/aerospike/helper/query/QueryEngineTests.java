package com.aerospike.helper.query;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ SelectorTest.class, FlightsTest.class, InserterTests.class, UpdatorTest.class, DeleterTests.class })
public class QueryEngineTests {
	public static final int PORT = 3000;
	public static final String HOST = "172.28.128.6";
	public static final String NAMESPACE = "test";
	public static final String SET_NAME = "selector";
	public static final int RECORD_COUNT = 100;

	public static final String AUTH_HOST = "C-25c35d91c6.aerospike.io";
	public static final int AUTH_PORT = 3200;
	public static final String AUTH_UID = "dbadmin";
	public static final String AUTH_PWD = "au4money";

}
