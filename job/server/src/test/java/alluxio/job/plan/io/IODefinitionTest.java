package alluxio.job.plan.io;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.collections.Pair;
import alluxio.conf.ServerConfiguration;
import alluxio.job.JobServerContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.job.plan.load.LoadDefinition;
import alluxio.stress.job.IOConfig;
import alluxio.stress.worker.WorkerBenchParameters;
import alluxio.underfs.UfsManager;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystem.class, JobServerContext.class, FileSystemContext.class,
        AlluxioBlockStore.class})
public class IODefinitionTest {
    private static final List<WorkerInfo> JOB_WORKERS = new ImmutableList.Builder<WorkerInfo>()
            .add(new WorkerInfo().setId(0).setAddress(new WorkerNetAddress().setHost("host0")))
            .add(new WorkerInfo().setId(1).setAddress(new WorkerNetAddress().setHost("host1")))
            .add(new WorkerInfo().setId(2).setAddress(new WorkerNetAddress().setHost("host2")))
            .add(new WorkerInfo().setId(3).setAddress(new WorkerNetAddress().setHost("host3"))).build();

    private JobServerContext mJobServerContext;
    private FileSystem mMockFileSystem;
    private AlluxioBlockStore mMockBlockStore;
    private FileSystemContext mMockFsContext;

    @Before
    public void before() {
        mMockFileSystem = PowerMockito.mock(FileSystem.class);
        mMockBlockStore = PowerMockito.mock(AlluxioBlockStore.class);
        mMockFsContext = PowerMockito.mock(FileSystemContext.class);
        PowerMockito.mockStatic(AlluxioBlockStore.class);
        PowerMockito.when(AlluxioBlockStore.create(any(FileSystemContext.class)))
                .thenReturn(mMockBlockStore);
        PowerMockito.when(mMockFsContext.getClientContext())
                .thenReturn(ClientContext.create(ServerConfiguration.global()));
        // TODO(jiacheng): need to hijack this conf?
        PowerMockito.when(mMockFsContext.getClusterConf()).thenReturn(ServerConfiguration.global());
        PowerMockito.when(mMockFsContext.getPathConf(any(AlluxioURI.class)))
                .thenReturn(ServerConfiguration.global());
        mJobServerContext = new JobServerContext(mMockFileSystem, mMockFsContext,
                Mockito.mock(UfsManager.class));
    }

    @Test
    public void selectExecutor() throws Exception {
        Set<WorkerInfo> workerSet = new HashSet<>(JOB_WORKERS);
        IOConfig jobConfig = new IOConfig(IOConfig.class.getCanonicalName(),
                ImmutableList.of(),
                0,
                16,
                1024,
                1,
                "hdfs://namenode:9000/alluxio");
        Set<Pair<WorkerInfo, ArrayList<String>>> assignments =
                new IODefinition().selectExecutors(jobConfig,
                        new ArrayList<>(JOB_WORKERS),
                        new SelectExecutorsContext(1, mJobServerContext));
        System.out.println(assignments);
        assertEquals(assignments.size(), 1);
        for (Pair<WorkerInfo, ArrayList<String>> p : assignments) {
            WorkerInfo w = p.getFirst();
            assertTrue(workerSet.contains(w));
        }
    }

    // Test join

    // Test exec command
    @Test
    public void execCommand() {
        Set<WorkerInfo> workerSet = new HashSet<>(JOB_WORKERS);
        IOConfig jobConfig = new IOConfig(IOConfig.class.getCanonicalName(),
                ImmutableList.of(),
                0,
                16,
                1024,
                1,
                "hdfs://namenode:9000/alluxio");
        IODefinition def = new IODefinition();
        // TODO(jiacheng): catch ShellUtils?
    }


}
