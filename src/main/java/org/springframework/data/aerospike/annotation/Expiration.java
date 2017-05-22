package org.springframework.data.aerospike.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Demarcates a property to be used as expiration field.
 * Expiration value must be TTL in seconds.
 * <br/><br/>
 * Client system time must be synchronized with aerospike server system time,
 * otherwise expiration behaviour will be unpredictable.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface Expiration {
}
