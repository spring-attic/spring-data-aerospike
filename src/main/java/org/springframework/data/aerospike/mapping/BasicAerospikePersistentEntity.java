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
import org.springframework.util.StringUtils;

/**
 * Default implementation of {@link AerospikePersistentEntity}.
 * 
 * @author Oliver Gierke
 */
public class BasicAerospikePersistentEntity<T> extends BasicPersistentEntity<T, AerospikePersistentProperty> implements
		AerospikePersistentEntity<T>, EnvironmentAware {

	static final int DEFAULT_EXPIRATION = 0;

	private Environment environment;
	private final TypeInformation<?> typeInformation;
	private final String defaultNameSpace;

	/**
	 * Creates a new {@link BasicAerospikePersistentEntity} using the given {@link TypeInformation}.
	 *
	 * @param information must not be {@literal null}.
	 * @param defaultNameSpace
	 */
	public BasicAerospikePersistentEntity(TypeInformation<T> information, String defaultNameSpace) {

		super(information);
		this.typeInformation = information;
		this.defaultNameSpace = defaultNameSpace;
	}

	@Override
	public String getNamespace() {
		return defaultNameSpace;
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
	public int getExpiration() {
		Document annotation = getType().getAnnotation(Document.class);
		if (annotation == null) {
			return DEFAULT_EXPIRATION;
		}

		int expiryValue = getExpiryValue(annotation);
		return (int) annotation.expiryUnit().toSeconds(expiryValue);
	}

	@Override
	public void setEnvironment(Environment environment) {
		this.environment = environment;
	}

	private int getExpiryValue(Document annotation) {
		int expiry = annotation.expiry();
		String expressionString = annotation.expiryExpression();

		if (StringUtils.hasLength(expressionString)) {
			Assert.state(expiry == DEFAULT_EXPIRATION, "Both 'expiry' and 'expiryExpression' are set");
			Assert.notNull(environment, "Environment must be set to use 'expiryExpression'");

			String resolvedExpression = environment.resolveRequiredPlaceholders(expressionString);
			try {
				return Integer.parseInt(resolvedExpression);
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException("Invalid Integer value for expiry expression: " + resolvedExpression);
			}
		}

		return expiry;
	}
}
