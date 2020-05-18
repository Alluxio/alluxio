package alluxio.stress.worker;

import alluxio.stress.Parameters;
import com.beust.jcommander.Parameter;

public class WorkerBenchParameters extends Parameters {
    @Parameter(names = {"--threads"}, description = "the number of threads to use")
    public int mThreads = 16;

    @Parameter(names = {"--io-size"},
            description = "size of data to write or read in total, in MB")
    public int mDataSize = 4096;

    @Parameter(names = {"--path"},
            description = "the Alluxio directory to write temporary data in",
            required = true)
    public String mPath;

    @Parameter(names = {"--workers"},
            description = "the number of workers to use")
    public int mWorkerNum=1;
}
