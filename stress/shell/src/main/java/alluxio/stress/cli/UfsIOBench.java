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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class UfsIOBench extends Benchmark<IOTaskResult> {
    private static final Logger LOG = LoggerFactory.getLogger(UfsIOBench.class);
    private static final int BUFFER_SIZE = 1024 * 1024;

    @ParametersDelegate
    private WorkerBenchParameters mParameters = new WorkerBenchParameters();

    private final InstancedConfiguration mConf = InstancedConfiguration.defaults();
    private final HashMap<String, String> mHdfsConf = new HashMap<>();

    @Override
    public PlanConfig generateJobConfig(String[] args) {
        // remove the cluster flag
        List<String> commandArgs =
                Arrays.stream(args).filter((s) -> !BaseParameters.CLUSTER_FLAG.equals(s))
                        .filter((s) -> !s.isEmpty()).collect(Collectors.toList());

        commandArgs.addAll(mBaseParameters.mJavaOpts);
        String className = this.getClass().getCanonicalName();
        return new IOConfig(className, commandArgs, mParameters);
    }

    @Override
    public IOTaskResult runLocal() throws Exception {
        ExecutorService pool =
                ExecutorServiceFactories.fixedThreadPool("bench-io-thread", mParameters.mThreads).create();

        IOTaskResult result = runIOBench(pool);

        pool.shutdownNow();
        pool.awaitTermination(30, TimeUnit.SECONDS);

        // Aggregate the task results
        return result;
    }

    @Override
    public void prepare() throws Exception {
        // TODO(jiacheng): what to set for hdfs conf?
    }

    /**
     * @param args command-line arguments
     */
    public static void main(String[] args) {
        mainInternal(args, new UfsIOBench());
    }

    private String getFilePathStr(int idx) {
        return mParameters.mPath + String.format("io-benchmark-%d", idx);
    }

    public IOTaskResult runIOBench(ExecutorService pool) throws Exception {
        IOTaskResult writeTaskResult = write(pool);
        IOTaskResult readTaskResult = read(pool);
        cleanUp();
        return writeTaskResult.merge(readTaskResult);
    }

    public void cleanUp() throws IOException {
        UnderFileSystemConfiguration ufsConf = UnderFileSystemConfiguration.defaults(mConf)
                .createMountSpecificConf(mHdfsConf);
        UnderFileSystem ufs = UnderFileSystem.Factory.create(mParameters.mPath, ufsConf);

        for (int i = 0; i < mParameters.mThreads; i++) {
            ufs.deleteFile(getFilePathStr(i));
        }
    }

    public IOTaskResult read(ExecutorService pool)
            throws IOException, InterruptedException, ExecutionException {
        // Use multiple threads to saturate the bandwidth of this worker
        int numThreads = mParameters.mThreads;

        UnderFileSystemConfiguration ufsConf = UnderFileSystemConfiguration.defaults(mConf)
                .createMountSpecificConf(mHdfsConf);
        UnderFileSystem ufs = UnderFileSystem.Factory.create(mParameters.mPath, ufsConf);
        if (!ufs.exists(mParameters.mPath)) {
            LOG.info("mkdirs {}", mParameters.mPath);
            ufs.mkdirs(mParameters.mPath);
        }

        List<CompletableFuture<IOTaskResult>> futures = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            final int idx = i;
            CompletableFuture<IOTaskResult> future = CompletableFuture.supplyAsync(() -> {
                IOTaskResult result = new IOTaskResult();
                result.setBaseParameters(mBaseParameters);
                result.setParameters(mParameters);
                long startTime = CommonUtils.getCurrentMs();

                String filePath = getFilePathStr(idx);
                LOG.info("filePath={}", filePath);

                int readMB = 0;
                try {
                    InputStream inStream = ufs.open(filePath);
                    byte[] buf = new byte[BUFFER_SIZE];
                    while (readMB < mParameters.mDataSize && inStream.read(buf) > 0) {
                        LOG.info("readMB={}", readMB);
                        readMB += 1; // 1 MB
                    }

                    long endTime = CommonUtils.getCurrentMs();
                    double duration = (endTime - startTime) / 1000.0; // convert to second
                    IOTaskResult.Point p = new IOTaskResult.Point(IOConfig.IOMode.READ, duration, readMB);
                    result.addPoint(p);
                    LOG.info("Read task finished {}", p);
                } catch (IOException e) {
                    LOG.error("Failed to read {}", filePath, e);
                    result.addError(e.getMessage());
                }

                return result;
            }, pool);
            futures.add(future);
        }

        // Collect the result
        CompletableFuture[] cfs = futures.toArray(new CompletableFuture[futures.size()]);
        List<IOTaskResult> results = CompletableFuture.allOf(cfs)
                .thenApply(f -> futures.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList())
                ).get();

        return IOTaskResult.reduceList(results);
    }

    public IOTaskResult write(ExecutorService pool)
            throws IOException, InterruptedException, ExecutionException {
        LOG.info("write()");

        // Use multiple threads to saturate the bandwidth of this worker
        int numThreads = mParameters.mThreads;
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
        final byte[] randomData = CommonUtils.randomBytes(BUFFER_SIZE);
        for (int i = 0; i < numThreads; i++) {
            final int idx = i;
            CompletableFuture<IOTaskResult> future = CompletableFuture.supplyAsync(() -> {
                IOTaskResult result = new IOTaskResult();
                result.setParameters(mParameters);
                result.setBaseParameters(mBaseParameters);
                long startTime = CommonUtils.getCurrentMs();

                String filePath = getFilePathStr(idx);
                LOG.info("filePath={}, data to write={}MB", filePath, mParameters.mDataSize);

                int wroteMB = 0;
                try {
                    OutputStream outStream = ufs.create(filePath);
                    LOG.info("OutputStream class {}", outStream.getClass().getCanonicalName());
                    while (wroteMB < mParameters.mDataSize) {
                        outStream.write(randomData);
                        LOG.info("Progress {}MB", wroteMB);
                        wroteMB += 1; // 1 MB
                        // TODO(jiacheng): when do i flush?
                        outStream.flush();
                    }

                    long endTime = CommonUtils.getCurrentMs();
                    double duration = (endTime - startTime) / 1000.0; // convert to second
                    IOTaskResult.Point p = new IOTaskResult.Point(IOConfig.IOMode.WRITE,
                            duration, wroteMB);
                    result.addPoint(p);
                    LOG.info("Write task finished {}", p);
                } catch (IOException e) {
                    LOG.error("Failed to write to UFS: ",e);
                    result.addError(e.getMessage());
                }

                LOG.info("Thread {} file={}, IOBench result={}", Thread.currentThread().getName(),
                        filePath, result);

                return result;

            }, pool);
            futures.add(future);
        }

        // Collect the result
        CompletableFuture[] cfs = futures.toArray(new CompletableFuture[futures.size()]);
        List<IOTaskResult> results = CompletableFuture.allOf(cfs)
                .thenApply(f -> futures.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList())
                ).get();

        return IOTaskResult.reduceList(results);
    }
}
