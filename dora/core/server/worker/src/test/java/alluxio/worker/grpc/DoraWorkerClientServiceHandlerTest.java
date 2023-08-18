package alluxio.worker.grpc;

import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.CacheManagerOptions;
import alluxio.client.file.cache.PageMetaStore;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.NotFoundRuntimeException;
import alluxio.grpc.*;
import alluxio.underfs.UfsStatus;
import alluxio.util.io.PathUtils;
import alluxio.worker.dora.DoraWorker;
import alluxio.worker.dora.PagedDoraWorker;
import io.grpc.stub.StreamObserver;
import io.grpc.stub.StreamObservers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import javax.validation.constraints.AssertTrue;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DoraWorkerClientServiceHandlerTest {

    private static final int LIST_STATUS_BATCH_SIZE =
            Configuration.getInt(PropertyKey.MASTER_FILE_SYSTEM_LISTSTATUS_RESULTS_PER_MESSAGE);
    @Rule
    public TemporaryFolder mTestFolder = new TemporaryFolder();
    private final long mPageSize =
            Configuration.global().getBytes(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE);
    private static final GetStatusPOptions GET_STATUS_OPTIONS_MUST_SYNC =
            GetStatusPOptions.newBuilder().setCommonOptions(
                    FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(0)).build();

    private DoraWorker mWorker;

    private DoraWorkerClientServiceHandler doraWorkerClientServiceHandler;

    @Mock
    private StreamObserver<ListStatusPResponse> listStatusPResponseStreamObserver;

    @Before
    public void before() throws Exception {
        // Initialize the mock objects
        MockitoAnnotations.initMocks(this);

        Configuration.set(PropertyKey.DORA_WORKER_METASTORE_ROCKSDB_DIR,
                mTestFolder.newFolder("rocks"));
        CacheManagerOptions cacheManagerOptions =
                CacheManagerOptions.createForWorker(Configuration.global());

        PageMetaStore pageMetaStore =
                PageMetaStore.create(CacheManagerOptions.createForWorker(Configuration.global()));
        CacheManager mCacheManager = CacheManager.Factory.create(Configuration.global(), cacheManagerOptions, pageMetaStore);
        mWorker = new PagedDoraWorker(new AtomicReference<>(1L), Configuration.global(), mCacheManager);
        doraWorkerClientServiceHandler = new DoraWorkerClientServiceHandler(mWorker);

    }

    @After
    public void after() throws Exception {
        mWorker.close();

    }

    @Test
    public void testListStatus() throws AccessControlException, IOException {
        // Create some sample data for testing
        File rootFolder = mTestFolder.newFolder("root");
        String rootPath = rootFolder.getAbsolutePath();
        mTestFolder.newFolder("root/d1");
        ListStatusPRequest listStatusPRequest = ListStatusPRequest.newBuilder().setPath(rootPath).build();
        UfsStatus[] statuses = mWorker.listStatus(rootPath, ListStatusPOptions.newBuilder().setRecursive(false).build()); // Initialize with valid statuses
        doraWorkerClientServiceHandler.listStatus(listStatusPRequest, listStatusPResponseStreamObserver);

        assert statuses != null;

        verify(listStatusPResponseStreamObserver, times(statuses.length / LIST_STATUS_BATCH_SIZE + 1)).onNext(any());
        verify(listStatusPResponseStreamObserver, times(1)).onCompleted();
        verify(listStatusPResponseStreamObserver, never()).onError(any());

        mWorker.delete(rootFolder.getAbsolutePath(), DeletePOptions.getDefaultInstance());
        mTestFolder.delete();
        // Assert that page cache, metadata cache & list cache all removed stale data
        listStatusPRequest = ListStatusPRequest.newBuilder().setPath(rootPath).build();
        statuses = mWorker.listStatus(rootPath, ListStatusPOptions.newBuilder().setRecursive(false).build()); // Initialize with valid statuses
        assert statuses == null;
//        when(mWorker.listStatus(rootPath, listStatusPRequest.getOptions())).thenThrow(new RuntimeException("Test exception"));
        doraWorkerClientServiceHandler.listStatus(listStatusPRequest, listStatusPResponseStreamObserver);

        verify(listStatusPResponseStreamObserver, times(1)).onError(any());
    }

}
