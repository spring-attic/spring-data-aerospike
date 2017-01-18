/**
 * Copyright 2012-2017 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.springframework.data.aerospike.example;

import java.util.Date;
import java.util.List;

import org.springframework.data.aerospike.example.data.CLRentPage;
import org.springframework.data.aerospike.repository.AerospikeRepository;

/**
 * @author Michael Zhang
 *
 */
public interface CLRentPageRepository extends AerospikeRepository<CLRentPage, String> {

	public List<CLRentPage> findAll();
	
	public List<CLRentPage> findByArea(Integer area);

	public List<CLRentPage> findByAreaBetween(int min, int max);
	
	public List<CLRentPage> findByGeoLocationWithin(double lon, double lat, double radius);

	public List<CLRentPage> findByPostDateAfterAndGeoLocationWithin(long postDate, double lon, double lat, double radius);
	public List<CLRentPage> findByGeoLocationWithinAndPostDateAfter(double lon, double lat, double radius, long postDate);

	public List<CLRentPage> findByPostDateAfter(long postDate);

}
