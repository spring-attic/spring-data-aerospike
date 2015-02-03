/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.data.aerospike.config;

import org.springframework.beans.factory.FactoryBean;

import com.aerospike.client.policy.ClientPolicy;

/**
 * A {@link FactoryBean} implementation that exposes the setters necessary to configure a {@link ClientPolicy} via XML.
 * TODO: add setters for all configurable properties.
 * 
 * @author Oliver Gierke
 */
public class ClientPolicyFactoryBean implements FactoryBean<ClientPolicy> {

	private final ClientPolicy policy;

	/**
	 * Creates a new {@link ClientPolicyFactoryBean}.
	 */
	public ClientPolicyFactoryBean() {
		this.policy = new ClientPolicy();
	}

	/**
	 * Configures the maximum number of concurrent threads for batch processing.
	 * 
	 * @param maxConcurrentThreads
	 */
	public void setMaxConcurrentThreads(int maxConcurrentThreads) {
		this.policy.batchPolicyDefault.maxConcurrentThreads = maxConcurrentThreads;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObject()
	 */
	@Override
	public ClientPolicy getObject() throws Exception {
		return policy;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#isSingleton()
	 */
	@Override
	public boolean isSingleton() {
		return false;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObjectType()
	 */
	@Override
	public Class<?> getObjectType() {
		return ClientPolicy.class;
	}
}
