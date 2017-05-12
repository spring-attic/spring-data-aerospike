package org.springframework.data.aerospike.convert;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.of;

/**
 * Value object to carry data to be written in object conversion.
 *
 * @author Oliver Gierke
 * @author Anastasiia Smirnova
 */
public class AerospikeWriteData {

	private Key key;
	private Collection<Bin> bins;

	public AerospikeWriteData(Key key, Collection<Bin> bins) {
		this.key = key;
		this.bins = bins;
	}

	public static AerospikeWriteData forWrite() {
		return new AerospikeWriteData(null, new ArrayList<>());
	}

	public void setKey(Key key) {
		this.key = key;
	}

	public Key getKey() {
		return key;
	}

	public Collection<Bin> getBins() {
		return Collections.unmodifiableCollection(bins);
	}

	public Bin[] getBinsAsArray() {
		return bins.toArray(new Bin[bins.size()]);
	}

	public void addBin(String key, Object value) {
		add(new Bin(key, value));
	}

	public void add(Bin bin) {
		this.bins.add(bin);
	}

}
