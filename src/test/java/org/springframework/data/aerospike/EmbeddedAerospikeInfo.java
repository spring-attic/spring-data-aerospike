package org.springframework.data.aerospike;

import lombok.AllArgsConstructor;
import lombok.Value;

@AllArgsConstructor
@Value
public class EmbeddedAerospikeInfo {

	String host;
	int port;
	String namespace;

}