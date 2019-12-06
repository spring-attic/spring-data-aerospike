package org.springframework.data.aerospike;

import org.awaitility.core.ThrowingRunnable;

import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.TEN_SECONDS;

public class AwaitilityUtils {

    public static void awaitTenSecondsUntil(ThrowingRunnable runnable) {
        await().atMost(TEN_SECONDS)
                .untilAsserted(runnable);
    }
}
