package org.springframework.data.aerospike.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.springframework.cache.Cache;
import org.springframework.cache.transaction.AbstractTransactionSupportingCacheManager;
import org.springframework.cache.transaction.TransactionAwareCacheDecorator;
import org.springframework.util.Assert;

import com.aerospike.client.AerospikeClient;

/**
 * 
 * @author Venil Noronha
 */
public class AerospikeCacheManager extends AbstractTransactionSupportingCacheManager {

	protected static final String DEFAULT_SET_NAME = "aerospike";
	
	private AerospikeClient aerospikeClient;
	private String setName;
	private Set<String> configuredCacheNames;

	public AerospikeCacheManager(AerospikeClient aerospikeClient) {
		this(aerospikeClient, Collections.<String>emptyList());
	}

	public AerospikeCacheManager(AerospikeClient aerospikeClient,
			Collection<String> cacheNames) {
		this(aerospikeClient, cacheNames, DEFAULT_SET_NAME);
	}

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
