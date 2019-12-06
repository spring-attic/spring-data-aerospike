package org.springframework.data.aerospike;

import org.springframework.dao.InvalidDataAccessResourceUsageException;

public class IndexAlreadyExistsException extends InvalidDataAccessResourceUsageException {

    public IndexAlreadyExistsException(String msg, Throwable cause) {
        super(msg, cause);
    }
}