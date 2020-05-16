package alluxio.stress.cli;

import alluxio.client.job.JobGrpcClientUtils;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.UnimplementedException;
import alluxio.job.plan.PlanConfig;
import alluxio.job.wire.JobInfo;
import alluxio.stress.BaseParameters;
import alluxio.stress.TaskResult;
import alluxio.stress.job.IOConfig;
import alluxio.stress.job.StressBenchConfig;
import alluxio.stress.master.MasterBenchParameters;
import alluxio.stress.master.MasterBenchTaskResult;
import alluxio.stress.worker.IOTaskResult;
import alluxio.stress.worker.WorkerBenchParameters;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;
import alluxio.util.FormatUtils;
import alluxio.util.ShellUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class UfsIOBench extends Benchmark<IOTaskResult> {
    private static final Logger LOG = LoggerFactory.getLogger(UfsIOBench.class);
    private static final long BUFFER_SIZE = 1024 * 1024;

    @ParametersDelegate
    private WorkerBenchParameters mParameters = new WorkerBenchParameters();

    private InstancedConfiguration mConf = InstancedConfiguration.defaults();

    @Override
    public PlanConfig generateJobConfig(String[] args) {
        // remove the cluster flag
        List<String> commandArgs =
                Arrays.stream(args).filter((s) -> !BaseParameters.CLUSTER_FLAG.equals(s))
                        .filter((s) -> !s.isEmpty()).collect(Collectors.toList());

        commandArgs.addAll(mBaseParameters.mJavaOpts);
        String className = this.getClass().getCanonicalName();
        return new IOConfig(className, commandArgs, 0, mParameters);
    }

    @Override
    public IOTaskResult runLocal() throws Exception {
        ExecutorService pool =
                ExecutorServiceFactories.fixedThreadPool("bench-io-thread", mParameters.mThreads).create();

        List<IOTaskResult> tr = new ArrayList<>();
        switch (mParameters.mMode) {
            case READ:
                tr.addAll(read(pool));
                break;
            case WRITE:
                tr.addAll(write(pool));
                break;
            case ALL:
                // TODO(jiacheng): read and write must be separated!
                tr.addAll(write(pool));
                tr.addAll(read(pool));
            default:
                throw new IllegalArgumentException(
                        String.format("Unknown mode %s", mParameters.mMode));
        }

        pool.shutdownNow();
        pool.awaitTermination(30, TimeUnit.SECONDS);

        // Aggregate the task results
        return IOTaskResult.reduceList(tr);
    }

    @Override
    public void prepare() throws Exception {

    }

    /**
     * @param args command-line arguments
     */
    public static void main(String[] args) {
        mainInternal(args, new UfsIOBench());
    }

    private String getFilePathStr(int idx) {
        return Paths.get(mParameters.mPath, String.format("io-benchmark-%d", idx))
                .normalize().toString();
    }

    public List<IOTaskResult> read(ExecutorService pool) throws Exception {
        // Use multiple threads to saturate the bandwidth of this worker
        int numThreads = mParameters.mThreads;
        final int toReadLength = mParameters.mDataSize;
        // TODO(jiacheng): need hdfs conf?
        Configuration hdfsConf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(mParameters.mPath), hdfsConf);

        List<CompletableFuture<IOTaskResult>> futures = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            final int idx = i;
            CompletableFuture<IOTaskResult> future = CompletableFuture.supplyAsync(() -> {
                IOTaskResult result = new IOTaskResult();
                long startTime = CommonUtils.getCurrentMs();

//                Path filePath = getFilePath(idx);
//                int readMB = 0;
//                try {
//                    FSDataInputStream inStream = fs.open(filePath);
//                    ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
//                    int len;
//                    while ((len = inStream.read(buffer)) != 0) {
//                        readMB += 1; // 1 MB
//                        // Discard the data read
//                        buffer.clear();
//                    }
//                } catch (IOException e) {
//                    result.addReadError(e);
//                }

                // If there are errors, the time spent in unsuccessful operations
                // are not ignored.
                long endTime = CommonUtils.getCurrentMs();
                result.setReadDurationMs(endTime - startTime);
//                result.setReadDataSize(readMB);

                return result;
            }, pool);
            futures.add(future);
        }

        // Collect the result
        CompletableFuture[] cfs = futures.toArray(new CompletableFuture[futures.size()]);
        List<IOTaskResult> result = CompletableFuture.allOf(cfs)
                .thenApply(f -> futures.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList())
                ).get();

        return result;
    }

    public List<IOTaskResult> write(ExecutorService pool) throws Exception {
        LOG.info("write()");

        // Use multiple threads to saturate the bandwidth of this worker
        int numThreads = mParameters.mThreads;
        final int toWriteLength = mParameters.mDataSize;
        // TODO(jiacheng): need hdfs conf?
        Map<String, String> hdfsConf = new HashMap<>();

        UnderFileSystemConfiguration ufsConf = UnderFileSystemConfiguration.defaults(mConf)
                .createMountSpecificConf(hdfsConf);
        UnderFileSystem ufs = UnderFileSystem.Factory.create(mParameters.mPath, ufsConf);
        if (!ufs.exists(mParameters.mPath)) {
            LOG.info("mkdirs {}", mParameters.mPath);
            ufs.mkdirs(mParameters.mPath);
        }

        List<CompletableFuture<IOTaskResult>> futures = new ArrayList<>();
        final byte[] randomData = CommonUtils.randomBytes(1024 * 1024);
        long fileSize = toWriteLength / numThreads;
        for (int i = 0; i < numThreads; i++) {
            final int idx = i;
            CompletableFuture<IOTaskResult> future = CompletableFuture.supplyAsync(() -> {
                IOTaskResult result = new IOTaskResult();
                long startTime = CommonUtils.getCurrentMs();

                String filePath = getFilePathStr(idx);
                LOG.info("filePath={}", filePath);

                int wroteMB = 0;
                try {
                    OutputStream outStream = ufs.create(filePath);
                    while (wroteMB < fileSize) {
                        outStream.write(randomData);
                        wroteMB += 1; // 1 MB
                    }
                } catch (IOException e) {
                    result.addWriteError(e);
                }

                // If there are errors, the metrics will mean nothing
                long endTime = CommonUtils.getCurrentMs();
                result.setWriteDurationMs(endTime - startTime);
                result.setWriteDataSize(wroteMB);

                LOG.info("Thread {} file={}, IOBench result={}", Thread.currentThread().getName(),
                        filePath, result);

                return result;

            }, pool);
            futures.add(future);
        }

        // Collect the result
        CompletableFuture[] cfs = futures.toArray(new CompletableFuture[futures.size()]);
        List<IOTaskResult> result = CompletableFuture.allOf(cfs)
                .thenApply(f -> futures.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList())
                ).get();

        return result;
    }
}
