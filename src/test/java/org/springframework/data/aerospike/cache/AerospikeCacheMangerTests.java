/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.aerospike.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.transaction.TransactionAwareCacheDecorator;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.aerospike.config.TestConfig;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.repository.BaseRepositoriesIntegrationTests;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;

/**
 * 
 * @author Venil Noronha
 */
public class AerospikeCacheMangerTests extends BaseRepositoriesIntegrationTests {

	@Autowired AerospikeClient client;
	@Autowired MappingAerospikeConverter converter;

	@Test
	public void testMissingCache() {
		AerospikeCacheManager manager = new AerospikeCacheManager(client, converter);
		manager.afterPropertiesSet();
		Cache cache = manager.getCache("missing-cache");
		assertNotNull("Cache instance was null", cache);
		assertTrue("Cache was not an instance of AerospikeCache", cache instanceof AerospikeCache);
	}

	@Test
	public void testDefaultCache() {
		AerospikeCacheManager manager = new AerospikeCacheManager(client,
				Arrays.asList("default-cache"), converter);
		manager.afterPropertiesSet();
		Cache cache = manager.lookupAerospikeCache("default-cache");
		assertNotNull("Cache instance was null", cache);
		assertTrue("Cache was not an instance of AerospikeCache", cache instanceof AerospikeCache);
	}

	@Test
	public void testDefaultCacheWithCustomizedSet() {
		AerospikeCacheManager manager = new AerospikeCacheManager(client,
				Arrays.asList("default-cache"), "custom-set", converter);
		manager.afterPropertiesSet();
		Cache cache = manager.lookupAerospikeCache("default-cache");
		assertNotNull("Cache instance was null", cache);
		assertTrue("Cache was not an instance of AerospikeCache", cache instanceof AerospikeCache);
	}

	@Test
	public void testTransactionAwareCache() {
		AerospikeCacheManager manager = new AerospikeCacheManager(client, converter);
		manager.setTransactionAware(true);
		manager.afterPropertiesSet();
		Cache cache = manager.getCache("transaction-aware-cache");
		assertNotNull("Cache instance was null", cache);
		assertTrue("Cache was not an instance of TransactionAwareCacheDecorator", cache instanceof TransactionAwareCacheDecorator);
	}

	@Test
	public void testCacheable() {
		cleanupForCacheableTest();
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(TestConfig.class);
		try {
			CachingComponent cachingComponent = ctx.getBean(CachingComponent.class);
			CachedObject response1 = cachingComponent.cachingMethod("foo");
			CachedObject response2 = cachingComponent.cachingMethod("foo");
			assertNotNull("Component returned null", response1);
			assertEquals("Response didn't match", "bar", response1.getValue());
			assertNotNull("Component returned null", response2);
			assertEquals("Response didn't match", "bar", response2.getValue());
			assertEquals("Component didn't cache result", 1, cachingComponent.getNoOfCalls());
		}
		finally {
			ctx.close();
			cleanupForCacheableTest();
		}
	}

	@Test
	public void testCacheEviction() {
		cleanupForCacheableTest();
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(TestConfig.class);
		try {
			CachingComponent cachingComponent = ctx.getBean(CachingComponent.class);
			CachedObject response1 = cachingComponent.cachingMethod("foo");
			cachingComponent.cacheEvictingMethod("foo");
			CachedObject response2 = cachingComponent.cachingMethod("foo");
			assertNotNull("Component returned null", response1);
			assertEquals("Response didn't match", "bar", response1.getValue());
			assertNotNull("Component returned null", response2);
			assertEquals("Response didn't match", "bar", response2.getValue());
			assertEquals("Component didn't evict cached entry", 2, cachingComponent.getNoOfCalls());
		}
		finally {
			ctx.close();
			cleanupForCacheableTest();
		}
	}

	private void cleanupForCacheableTest() {
		client.delete(null, new Key(getNameSpace(), AerospikeCacheManager.DEFAULT_SET_NAME, "foo"));
	}

	public static class CachedObject {
		private String value;

		public CachedObject(String value) {
			this.value = value;
		}

		public String getValue() {
			return value;
		}
	}

	public static class CachingComponent {
		private int noOfCalls = 0;

		@Cacheable("test")
		public CachedObject cachingMethod(String param) {
			noOfCalls ++;
			return new CachedObject("bar");
		}

		@CacheEvict("test")
		public void cacheEvictingMethod(String param) {

		}

		public int getNoOfCalls() {
			return noOfCalls;
		}
	}

}
