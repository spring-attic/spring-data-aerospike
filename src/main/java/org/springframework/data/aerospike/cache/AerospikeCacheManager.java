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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.transaction.AbstractTransactionSupportingCacheManager;
import org.springframework.cache.transaction.TransactionAwareCacheDecorator;
import org.springframework.util.Assert;

import com.aerospike.client.AerospikeClient;

/**
 * {@link CacheManager} implementation for Aerospike. By default {@link AerospikeCache}s
 * will be lazily initialized for each {@link #getCache(String)} request unless a set of
 * predefined cache names is provided. <br>
 * <br>
 * Setting {@link #setTransactionAware(boolean)} to <code>true</code> will force Caches to
 * be decorated as {@link TransactionAwareCacheDecorator} so values will only be written
 * to the cache after successful commit of surrounding transaction.
 * 
 * @author Venil Noronha
 */
public class AerospikeCacheManager extends AbstractTransactionSupportingCacheManager {

	protected static final String DEFAULT_SET_NAME = "aerospike";

	private AerospikeClient aerospikeClient;
	private String setName;
	private Set<String> configuredCacheNames;

	/**
	 * Create a new {@link AerospikeCacheManager} instance with no caches and with the
	 * set name "aerospike".
	 * 
	 * @param aerospikeClient the {@link AerospikeClient} instance.
	 */
	public AerospikeCacheManager(AerospikeClient aerospikeClient) {
		this(aerospikeClient, Collections.<String>emptyList());
	}

	/**
	 * Create a new {@link AerospikeCacheManager} instance with no caches and with the
	 * specified set name.
	 * 
	 * @param aerospikeClient the {@link AerospikeClient} instance.
	 * @param setName the set name.
	 */
	public AerospikeCacheManager(AerospikeClient aerospikeClient, String setName) {
		this(aerospikeClient, Collections.<String>emptyList(), setName);
	}

	/**
	 * Create a new {@link AerospikeCacheManager} instance with the specified caches and
	 * with the set name "aerospike".
	 * 
	 * @param aerospikeClient the {@link AerospikeClient} instance.
	 * @param cacheNames the default caches to create.
	 */
	public AerospikeCacheManager(AerospikeClient aerospikeClient,
			Collection<String> cacheNames) {
		this(aerospikeClient, cacheNames, DEFAULT_SET_NAME);
	}

	/**
	 * Create a new {@link AerospikeCacheManager} instance with the specified caches and
	 * with the specified set name.
	 * 
	 * @param aerospikeClient the {@link AerospikeClient} instance.
	 * @param cacheNames the default caches to create.
	 * @param setName the set name.
	 */
	public AerospikeCacheManager(AerospikeClient aerospikeClient,
			Collection<String> cacheNames, String setName) {
		Assert.notNull(aerospikeClient, "AerospikeClient must not be null");
		Assert.notNull(cacheNames, "Cache names must not be null");
		Assert.notNull(setName, "Set name must not be null");
		this.aerospikeClient = aerospikeClient;
		this.setName = setName;
		this.configuredCacheNames = new LinkedHashSet<String>(cacheNames);
	}

	@Override
	protected Collection<? extends Cache> loadCaches() {
		List<AerospikeCache> caches = new ArrayList<AerospikeCache>();
		for (String cacheName : configuredCacheNames) {
			caches.add(createCache(cacheName));
		}
		return caches;
	}

	@Override
	protected Cache getMissingCache(String cacheName) {
		return createCache(cacheName);
	}

	protected AerospikeCache createCache(String cacheName) {
		return new AerospikeCache(cacheName, setName, aerospikeClient, -1);
	}

	@Override
	public Cache getCache(String name) {
		Cache cache = lookupAerospikeCache(name);
		if (cache != null) {
			return cache;
		}
		else {
			Cache missingCache = getMissingCache(name);
			if (missingCache != null) {
				addCache(missingCache);
				return lookupAerospikeCache(name);  // may be decorated
			}
			return null;
		}
	}

	protected Cache lookupAerospikeCache(String name) {
		return lookupCache(name + ":" + setName);
	}

	@Override
	protected Cache decorateCache(Cache cache) {
		if (isCacheAlreadyDecorated(cache)) {
			return cache;
		}
		return super.decorateCache(cache);
	}

	protected boolean isCacheAlreadyDecorated(Cache cache) {
		return isTransactionAware() && cache instanceof TransactionAwareCacheDecorator;
	}

}
