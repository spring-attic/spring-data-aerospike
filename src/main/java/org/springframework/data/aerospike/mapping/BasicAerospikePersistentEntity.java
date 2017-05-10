/*******************************************************************************
 * Copyright (c) 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 * 		http://www.apache.org/licenses/LICENSE-2.0
 *  	
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.springframework.data.aerospike.mapping;

import com.aerospike.client.Key;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link AerospikePersistentEntity}.
 * 
 * @author Oliver Gierke
 */
public class BasicAerospikePersistentEntity<T> extends BasicPersistentEntity<T, AerospikePersistentProperty> implements
		AerospikePersistentEntity<T>, EnvironmentAware {

	private Environment environment;
	private final TypeInformation<?> typeInformation;

	/**
	 * Creates a new {@link BasicAerospikePersistentEntity} using the given {@link TypeInformation}.
	 * 
	 * @param information must not be {@literal null}.
	 */
	public BasicAerospikePersistentEntity(TypeInformation<T> information) {

		super(information);
		this.typeInformation = information;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.aerospike.mapping.AerospikePersistentEntity#getSetName()
	 */
	@Override
	public String getSetName() {
		return AerospikeSimpleTypes.getColletionName(typeInformation.getType());
	}

	@Override
	public Key getKey() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getGeneration() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getTTL() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void setEnvironment(Environment environment) {
		this.environment = environment;
	}
}
