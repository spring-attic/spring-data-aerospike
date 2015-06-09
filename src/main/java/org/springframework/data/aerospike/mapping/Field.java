package org.springframework.data.aerospike.mapping;

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
public @interface Field {

	/**
	 * The key to be used to store the field inside the document.
	 * 
	 * @return
	 */
	String value() default "";

}
