package alluxio.client.file.cache.filter;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public final class Constants {
    public static final int K = 1000;
    public static final int M = K * 1000;

    public static final int KB = 1024;
    public static final int MB = KB * 1024;
    public static final int GB = MB * 1024;
    public static final long TB = GB * 1024L;
    public static final long PB = TB * 1024L;

    public static final long SECOND = 1000;
    public static final long MINUTE = SECOND * 60L;
    public static final long HOUR = MINUTE * 60L;
    public static final long DAY = HOUR * 24L;

    // Cuckoo constants
    public static final int MAX_BFS_PATH_LEN = 5;
}
