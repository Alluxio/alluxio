package alluxio.stress.worker;

import alluxio.stress.Parameters;
import com.beust.jcommander.Parameter;

public class WorkerBenchParameters extends Parameters {
    @Parameter(names = {"--mode"},
            description = "WRITE, READ or ALL",
            required = true)
    public IOMode mMode;

    @Parameter(names = {"--threads"}, description = "the number of threads to use")
    public int mThreads = 16;

    @Parameter(names = {"--io-size"},
            description = "size of data to write or read in total, in MB")
    public int mDataSize = 4096;

    @Parameter(names = {"--ufs-path"},
            description = "the directory to write temporary data in",
            required = true)
    public String mUfsTempDirPath;

    @Parameter(names = {"--workers"},
            description = "the number of workers to use")
    public int mWorkerNum;

    public enum IOMode {
        READ,
        WRITE,
        ALL
    }
}
