package alluxio.shuttle;

public class Constants {
    /**
     *  Timeout ms for thread pause
     */
    public static final long PAUSE_TIMEOUT_MS = 300;

    /**
     * Status for thread of SingleThreadExecutor
     */
    public enum ThreadStatus {
        INIT(1),
        RUNNING(2),
        SHUTDOWN(3),
        STOP(4);

        private int value;

        ThreadStatus(int value) {
            this.value = value;
        }

        public int getValue() {
            return this.value;
        }
    };

    public static final int DEFAULT_MSG_QUEUE_SIZE = 1024;
    public final static int DEFAULT_GROUP_THREAD_NUM = Math.max(4, 2 * Runtime.getRuntime().availableProcessors());
}
