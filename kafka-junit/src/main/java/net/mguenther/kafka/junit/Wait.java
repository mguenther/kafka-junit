package net.mguenther.kafka.junit;

import java.util.concurrent.TimeUnit;

public class Wait {

    /**
     * Delays the thread that the test runs in by the given duration in seconds.
     *
     * @param duration
     *      the duration of the delay in seconds
     * @throws InterruptedException
     *      in case an interrupted signal is set
     */
    public static void delay(final int duration) throws InterruptedException {
        delay(duration, TimeUnit.SECONDS);
    }

    /**
     * Delays the thread that the test runs in by the given duration.
     *
     * @param duration
     *      the duration of the delay
     * @param timeUnit
     *      the time unit for the given {@code duration}
     * @throws InterruptedException
     *      in case an interrupted signal is set
     */
    public static void delay(final int duration, final TimeUnit timeUnit) throws InterruptedException {
        Thread.sleep(timeUnit.toMillis(duration));
    }
}
