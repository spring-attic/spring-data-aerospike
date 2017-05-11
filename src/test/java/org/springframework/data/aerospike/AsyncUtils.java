package org.springframework.data.aerospike;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

public class AsyncUtils {

	public static void executeConcurrently(int numThreads, Callable<Void> task) throws Exception {
		ExecutorService pool = Executors.newFixedThreadPool(numThreads);
		try {
			Collection<Callable<Void>> tasks = Collections.nCopies(numThreads, task);

			List<Future<Void>> futures = pool.invokeAll(tasks);
			for (Future future : futures) {
				future.get(5, TimeUnit.SECONDS);
			}
		} finally {
			pool.shutdownNow();
		}
	}
}
