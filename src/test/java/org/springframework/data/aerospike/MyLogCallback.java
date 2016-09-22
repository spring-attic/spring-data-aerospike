/**
 * 
 */
package org.springframework.data.aerospike;

import java.util.Date;

import com.aerospike.client.Log;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
public final class MyLogCallback implements Log.Callback {
	@Override
	public void log(Log.Level level, String message) {
		Date date = new Date();
		System.out.println(date.toString() + ' ' + level + ' ' + message);
	}
}
