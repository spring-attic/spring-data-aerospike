package org.springframework.data.aerospike;

import org.springframework.dao.InvalidDataAccessResourceUsageException;

public class IndexNotFoundException extends InvalidDataAccessResourceUsageException {

    public IndexNotFoundException(String msg, Throwable cause) {
        super(msg, cause);
    }
}