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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.cache.Cache;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.CachingConfigurerSupport;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.transaction.TransactionAwareCacheDecorator;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;

/**
 * 
 * @author Venil Noronha
 */
public class AerospikeCacheMangerTests {

	private static AerospikeClient client;

	@BeforeClass
	public static void setUp() {
		client = new AerospikeClient("localhost", 3000);
	}
	
	@AfterClass
	public static void tearDown() {
		client.close();
	}
	
	@Test
	public void testMissingCache() {
		AerospikeCacheManager manager = new AerospikeCacheManager(client);
		manager.afterPropertiesSet();
		Cache cache = manager.getCache("missing-cache");
		assertNotNull("Cache instance was null", cache);
		assertTrue("Cache was not an instance of AerospikeCache", cache instanceof AerospikeCache);
	}
	
	@Test
	public void testDefaultCache() {
		AerospikeCacheManager manager = new AerospikeCacheManager(client,
				Arrays.asList("default-cache"));
		manager.afterPropertiesSet();
		Cache cache = manager.lookupAerospikeCache("default-cache");
		assertNotNull("Cache instance was null", cache);
		assertTrue("Cache was not an instance of AerospikeCache", cache instanceof AerospikeCache);
	}
	
	@Test
	public void testDefaultCacheWithCustomizedSet() {
		AerospikeCacheManager manager = new AerospikeCacheManager(client,
				Arrays.asList("default-cache"), "custom-set");
		manager.afterPropertiesSet();
		Cache cache = manager.lookupAerospikeCache("default-cache");
		assertNotNull("Cache instance was null", cache);
		assertTrue("Cache was not an instance of AerospikeCache", cache instanceof AerospikeCache);
	}
	
	@Test
	public void testTransactionAwareCache() {
		AerospikeCacheManager manager = new AerospikeCacheManager(client);
		manager.setTransactionAware(true);
		manager.afterPropertiesSet();
		Cache cache = manager.getCache("transaction-aware-cache");
		assertNotNull("Cache instance was null", cache);
		assertTrue("Cache was not an instance of TransactionAwareCacheDecorator", cache instanceof TransactionAwareCacheDecorator);
	}
	
	@Test
	public void testCacheable() {
		cleanupForCacheableTest();
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(CachingConfiguration.class);
		try {
			CachingComponent cachingComponent = ctx.getBean(CachingComponent.class);
			cachingComponent.cachingMethod("foo");
			cachingComponent.cachingMethod("foo");
			assertEquals("Component didn't cache result", cachingComponent.getNoOfCalls(), 1);
		}
		finally {
			ctx.close();
			cleanupForCacheableTest();
		}
	}
	
	@Test
	public void testCacheEviction() {
		cleanupForCacheableTest();
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(CachingConfiguration.class);
		try {
			CachingComponent cachingComponent = ctx.getBean(CachingComponent.class);
			cachingComponent.cachingMethod("foo");
			cachingComponent.cacheEvictingMethod("foo");
			cachingComponent.cachingMethod("foo");
			assertEquals("Component didn't evict cached entry", cachingComponent.getNoOfCalls(), 2);
		}
		finally {
			ctx.close();
			cleanupForCacheableTest();
		}
	}

	private void cleanupForCacheableTest() {
		client.delete(null, new Key("test", AerospikeCacheManager.DEFAULT_SET_NAME, "foo"));
	}
	
	public static class CachingComponent {
		
		private int noOfCalls = 0;
		
		@Cacheable("test")
		public String cachingMethod(String param) {
			noOfCalls ++;
			return "bar";
		}
		
		@CacheEvict("test")
		public void cacheEvictingMethod(String param) {
			
		}

		public int getNoOfCalls() {
			return noOfCalls;
		}
		
	}
	
	@Configuration
	@EnableCaching
	public static class CachingConfiguration extends CachingConfigurerSupport {
		
		@Bean
		public AerospikeCacheManager cacheManager() {
			return new AerospikeCacheManager(client);
		}
		
		@Bean
		public CachingComponent cachingComponent() {
			return new CachingComponent();
		}
		
	}
	
}
