package alluxio.shuttle;

public class Constants {

    public static final int SHUTTLE_RPC_CLIENT_CONN_TIMEOUT_MS = 120_000;
    public static final int SHUTTLE_RPC_CLIENT_THREAD_NUM_DEFAULT = 16;
    public static final int SHUTTLE_RPC_CLIENT_CONN_NUM_DEFAULT = 1;

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
