package com.aerospike.helper.query;

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;

public class FlightsTest {
	private static final String NAMESPACE = "test";
	private static final String SET_NAME = "flights";
	AerospikeClient client;
	QueryEngine selector;

	@Before
	public void setUp() throws Exception {
		client = new AerospikeClient("172.28.128.6", 3000);
		selector = new QueryEngine(client);
		selector.refreshCluster();
	}

	@After
	public void tearDown() throws Exception {
		selector.close();
	}

	@Test
	public void selectNoQualifiers() throws IOException {
		Statement st = new Statement();
		st.setNamespace(NAMESPACE);
		st.setSetName(SET_NAME);
		st.setBinNames("ORIGIN", "DEST", "CARRIER", "FL_NUM");
		KeyRecordIterator it = selector.select(st);
		int count = 0;
		while (it.hasNext()){
			KeyRecord rec = it.next();
			count++;
			//System.out.println(rec);
		}
		it.close();
		//System.out.println(count);
		Assert.assertTrue(count > 900000);
	}

	@Test
	public void selectWith2Qualifiers() throws IOException {
		Qualifier qual1 = new Qualifier("ORIGIN", Qualifier.FilterOperation.EQ, Value.get("BWI"));
		Qualifier qual2 = new Qualifier("DEST", Qualifier.FilterOperation.EQ, Value.get("JFK"));
		Statement st = new Statement();
		st.setNamespace(NAMESPACE);
		st.setSetName(SET_NAME);
		st.setBinNames("ORIGIN", "DEST", "CARRIER", "FL_NUM");
		KeyRecordIterator it = selector.select(st, qual1, qual2);
		int count = 0;
		while (it.hasNext()){
			KeyRecord rec = it.next();
			count++;
			//			System.out.println(rec);
		}
		it.close();
		//		System.out.println(count);
		Assert.assertEquals(62, count);
	}
	@Test
	public void selectWith3Qualifiers() throws IOException {
		Qualifier qual1 = new Qualifier("ORIGIN", Qualifier.FilterOperation.EQ, Value.get("SFO"));
		Qualifier qual2 = new Qualifier("DEST", Qualifier.FilterOperation.EQ, Value.get("JFK"));
		Qualifier qual3 = new Qualifier("CARRIER", Qualifier.FilterOperation.EQ, Value.get("UA"));
		Statement st = new Statement();
		st.setNamespace(NAMESPACE);
		st.setSetName(SET_NAME);
		st.setBinNames("ORIGIN", "DEST", "CARRIER", "FL_NUM");
		KeyRecordIterator it = selector.select(st, qual1, qual2, qual3);
		int count = 0;
		while (it.hasNext()){
			KeyRecord rec = it.next();
			count++;
			//System.out.println(rec);
		}
		it.close();
		//System.out.println(count);
		Assert.assertEquals(354, count);
	}
	@Test
	public void selectWithRangeNoIndex() throws IOException {
		//select FL_DATE_BIN, ORIGIN, DEST, CARRIER, FL_NUM from test.flights where FL_DATE_BIN between 1325570400 and 1326261600
		long start = System.currentTimeMillis();
		Qualifier qual1 = new Qualifier("FL_DATE_BIN", Qualifier.FilterOperation.BETWEEN, Value.get(1325570400), Value.get(1326261600));
		Statement st = new Statement();
		st.setNamespace(NAMESPACE);
		st.setSetName(SET_NAME);
		st.setBinNames("FL_DATE_BIN", "ORIGIN", "DEST", "CARRIER", "FL_NUM");
		KeyRecordIterator it = selector.select(st, qual1);
		while (it.hasNext()){
			KeyRecord rec = it.next();
			long date = rec.record.getLong("FL_DATE_BIN");
			Assert.assertTrue((132557040 <= date) && (date <= 1326261600));
		}
		it.close();
		long stop = System.currentTimeMillis();
		System.out.println(String.format("Non-indexed Execution time:%d", stop-start));
	}
	@Test
	public void selectWithRangeIndex() throws IOException {
		//select FL_DATE_BIN, ORIGIN, DEST, CARRIER, FL_NUM from test.flights where FL_DATE_BIN between 1325570400 and 1326261600
		IndexTask task = this.selector.client.createIndex(null, "test", "flights", "flights_index", "FL_DATE_BIN", IndexType.NUMERIC);
		task.waitTillComplete(5000);
		this.selector.refreshIndexes();
		long start = System.currentTimeMillis();
		Qualifier qual1 = new Qualifier("FL_DATE_BIN", Qualifier.FilterOperation.BETWEEN, Value.get(1325570400), Value.get(1326261600));
		Statement st = new Statement();
		st.setNamespace(NAMESPACE);
		st.setSetName(SET_NAME);
		st.setBinNames("FL_DATE_BIN", "ORIGIN", "DEST", "CARRIER", "FL_NUM");
		KeyRecordIterator it = selector.select(st, qual1);
		while (it.hasNext()){
			KeyRecord rec = it.next();
			long date = rec.record.getLong("FL_DATE_BIN");
			Assert.assertTrue((132557040 <= date) && (date <= 1326261600));
		}
		it.close();
		long stop = System.currentTimeMillis();
		System.out.println(String.format("Indexed Execution time:%d", stop-start));

		this.selector.client.dropIndex(null, "test", "flights", "flights_index");
	}

	@Test
	public void selectWithGTLimit() throws IOException {

		//select * from test.flights where DISTANCE > 400
		Qualifier qual1 = new Qualifier("DISTANCE", Qualifier.FilterOperation.GT, Value.get(400));
		Statement st = new Statement();
		st.setNamespace(NAMESPACE);
		st.setSetName(SET_NAME);
		st.setBinNames("FL_DATE_BIN", "ORIGIN", "DEST", "CARRIER", "FL_NUM", "DISTANCE");
		int count =0;
		KeyRecordIterator it = selector.select(st, qual1);
		try {
			while (it.hasNext()){
				count++;
				KeyRecord rec = it.next();
				long dist = rec.record.getLong("DISTANCE");
				Assert.assertTrue(dist > 400);
				if (count == 10)
					break;
			}
		} finally {
			it.close();
		}
		Assert.assertEquals(10, count);
	}
}
