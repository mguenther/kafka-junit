package net.mguenther.kafka.junit;

public interface EmbeddedLifecycle {

    /**
     * Starts the embedded component. After this method completes the component *must* fully
     * operational.
     *
     * @throws RuntimeException
     *      this method can and should throw an {@link RuntimeException} to indicate that the
     *      component could not be deployed
     */
    void start();

    /**
     * Stops the embedded component. After this method completes all acquired resources are
     * freed and the component is properly shut down. The component is no longer operational
     * after this.
     *
     * @throws RuntimeException
     *      this method can and should throw an {@link RuntimeException} to indicate that the
     *      component could not be shut down
     */
    void stop();
}
