package org.springframework.data.aerospike.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Annotation to define custom on fields.
 * 
 * @author Peter Milne
 
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface Bin {

	/**
	 * The bin name to store the field inside the record.
	 * 
	 * @return
	 */
	String value() default "";

}
