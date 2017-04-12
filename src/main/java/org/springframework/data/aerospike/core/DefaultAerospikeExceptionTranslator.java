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
package org.springframework.data.aerospike.core;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.data.keyvalue.core.UncategorizedKeyValueException;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;

/**
 * @author Peter Milne
 * This class translates the AerospikeException and result code
 * to a DataAccessException.
 */
public class DefaultAerospikeExceptionTranslator implements AerospikeExceptionTranslator {

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.dao.support.PersistenceExceptionTranslator#translateExceptionIfPossible(java.lang.RuntimeException)
	 */
	@Override
	public DataAccessException translateExceptionIfPossible(RuntimeException cause) {

		if (cause instanceof AerospikeException){
			int resultCode = ((AerospikeException)cause).getResultCode();
			String msg = ((AerospikeException)cause).getMessage();
			switch (resultCode) {
			/*
			 * Future enhancements will be more elaborate 
			 */
			case ResultCode.KEY_EXISTS_ERROR:
				return new DuplicateKeyException(msg, cause);
			default:
				return new RecoverableDataAccessException("Aerospike Error: " + cause.getMessage(), cause);

			}
		}
		return new UncategorizedKeyValueException("Unexpected Aerospike Exception", cause);
	}
}
