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
package org.springframework.data.aerospike.example.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.springframework.data.annotation.Id;

import com.aerospike.client.Value.GeoJSONValue;

/**
 * @author Michael Zhang
 *
 */
public class CLRentPage implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private @Id String id;
	
	private GeoJSONValue geoLocation;
	
	private float price;
	
	private List<String> images;
	
	private long postDate;
	
	private int area;
	
	private Float bedroom;
	
	private Float bath;
	
	private String phone;
	
	private String email;
	
	private String postingBody;

	/**
	 * @param id
	 */
	public CLRentPage(String id) {
		super();
		this.id = id;
		images = new ArrayList<String>();
	}

	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}

	/**
	 * @param id the id to set
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * @return the price
	 */
	public float getPrice() {
		return price;
	}

	/**
	 * @param price the price to set
	 */
	public void setPrice(float price) {
		this.price = price;
	}

	/**
	 * @return the images
	 */
	public List<String> getImages() {
		return images;
	}

	/**
	 * @param images the images to set
	 */
	public void setImages(List<String> images) {
		this.images = images;
	}

	/**
	 * @return the postDate
	 */
	public long getPostDate() {
		return postDate;
	}

	/**
	 * @param postDate the postDate to set
	 */
	public void setPostDate(long postDate) {
		this.postDate = postDate;
	}

	/**
	 * @return the area
	 */
	public float getArea() {
		return area;
	}

	/**
	 * @param area the area to set
	 */
	public void setArea(int area) {
		this.area = area;
	}

	/**
	 * @return the bedroom
	 */
	public Float getBedroom() {
		return bedroom;
	}

	/**
	 * @param bedroom the bedroom to set
	 */
	public void setBedroom(Float bedroom) {
		this.bedroom = bedroom;
	}

	/**
	 * @return the bath
	 */
	public Float getBath() {
		return bath;
	}

	/**
	 * @param bath the bath to set
	 */
	public void setBath(Float bath) {
		this.bath = bath;
	}
	
	/**
	 * @return the phone
	 */
	public String getPhone() {
		return phone;
	}

	/**
	 * @param phone the phone to set
	 */
	public void setPhone(String phone) {
		this.phone = phone;
	}

	/**
	 * @return the email
	 */
	public String getEmail() {
		return email;
	}

	/**
	 * @param email the email to set
	 */
	public void setEmail(String email) {
		this.email = email;
	}

	/**
	 * @return the geoLocation
	 */
	public GeoJSONValue getGeoLocation() {
		return geoLocation;
	}

	/**
	 * @param geoLocation the geoLocation to set
	 */
	public void setGeoLocation(GeoJSONValue geoLocation) {
		this.geoLocation = geoLocation;
	}

	/**
	 * @return the postingBody
	 */
	public String getPostingBody() {
		return postingBody;
	}

	/**
	 * @param postingBody the postingBody to set
	 */
	public void setPostingBody(String postingBody) {
		this.postingBody = postingBody;
	}

	@Override
	public String toString() {
		return "Rent Page [id=" + id + ", price=" + price + ", SF=" + area + ", bed/bath=" + bedroom + "/" + bath + "]";
	}

}
