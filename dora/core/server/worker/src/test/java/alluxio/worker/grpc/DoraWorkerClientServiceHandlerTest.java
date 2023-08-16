package alluxio.worker.grpc;

import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.CacheManagerOptions;
import alluxio.client.file.cache.PageMetaStore;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.*;
import alluxio.underfs.UfsStatus;
import alluxio.worker.dora.DoraWorker;
import alluxio.worker.dora.PagedDoraWorker;
import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DoraWorkerClientServiceHandlerTest {

    @Rule
    public TemporaryFolder mTestFolder = new TemporaryFolder();
    private CacheManager mCacheManager;
    private final long mPageSize =
            Configuration.global().getBytes(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE);
    private static final GetStatusPOptions GET_STATUS_OPTIONS_MUST_SYNC =
            GetStatusPOptions.newBuilder().setCommonOptions(
                    FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(0)).build();

    @Mock
    private DoraWorker mWorker;

    private DoraWorkerClientServiceHandler handler;

    @Before
    public void Before() throws IOException {
        Configuration.set(PropertyKey.DORA_WORKER_METASTORE_ROCKSDB_DIR,
                mTestFolder.newFolder("rocks"));
        CacheManagerOptions cacheManagerOptions =
                CacheManagerOptions.createForWorker(Configuration.global());

        PageMetaStore pageMetaStore =
                PageMetaStore.create(CacheManagerOptions.createForWorker(Configuration.global()));
        mCacheManager =
                CacheManager.Factory.create(Configuration.global(), cacheManagerOptions, pageMetaStore);
        mWorker = new PagedDoraWorker(new AtomicReference<>(1L), Configuration.global(), mCacheManager);
        handler = new DoraWorkerClientServiceHandler(mWorker);
    }

    @After
    public void after() throws Exception {
        mWorker.close();
    }
    @Test
    public void testListStatus() throws Exception {
        // Create a ListStatusPRequest
        File rootFolder = mTestFolder.newFolder("root");
        String rootPath = rootFolder.getAbsolutePath();

        ListStatusPRequest request = ListStatusPRequest.newBuilder()
                .setPath(rootPath)
                .setOptions(ListStatusPOptions.getDefaultInstance())
                .build();

        UfsStatus[] mockStatuses = new UfsStatus[1]; // Mock UfsStatus array
        when(handler.listStatus(eq("/your/test/path"), any())).thenReturn(mockStatuses);

        // Mock responseObserver
        StreamObserver<ListStatusPResponse> responseObserver = mock(StreamObserver.class);

        // Call the method to be tested
        handler.listStatus(request, responseObserver);

        // Verify that mWorker.listStatus was called with the correct arguments
        verify(mWorker).listStatus(eq(rootPath), any());

        // Verify that responseObserver methods were called appropriately
        verify(responseObserver).onNext(any(ListStatusPResponse.class));
        verify(responseObserver).onCompleted();
        verifyNoMoreInteractions(responseObserver); // Verify no unexpected interactions
    }

    // Add more test methods for other methods in DoraWorkerClientServiceHandler
}
