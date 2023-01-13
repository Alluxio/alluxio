package alluxio.worker.page;


import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.*;
import alluxio.Constants;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.CacheManagerOptions;
import alluxio.client.file.cache.evictor.CacheEvictorOptions;
import alluxio.client.file.cache.evictor.FIFOCacheEvictor;
import alluxio.client.file.cache.store.PageStoreDir;
import alluxio.client.file.cache.store.PageStoreOptions;
import alluxio.client.file.cache.store.PageStoreType;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.master.NoopUfsManager;
import alluxio.underfs.UfsManager;
import alluxio.worker.block.*;
import alluxio.worker.block.io.BlockWriter;
import com.google.common.collect.ImmutableList;
import io.grpc.Status;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import java.nio.file.Path;

import static org.junit.Assert.assertEquals;

public class PagedBlockStoreCommitBlockTest {
    BlockStoreEventListener mListener;
    UfsManager mUfs;
    AlluxioConfiguration conf;
    CacheManagerOptions cacheManagerOptions;
    PagedBlockMetaStore pageMetaStore;
    List<PagedBlockStoreDir> dirs;
    PagedBlockStore pagedBlockStore;
    BlockMasterClientPool mBlockMasterClientPool;
    BlockMasterClient mockedBlockMasterClient;
    AtomicReference<Long> workerId;

    CacheManager cacheManager;

    private static final int DIR_INDEX = 0;

    final Long mSessionId = 1L;
    final Long mBlockId = 2L;
    final int mBlockSize = 64;

    public int mPageSize = 2;

    private static final int offset = 0;

    private PagedBlockStoreDir mDir;

    @Rule
    public TemporaryFolder mTempFolder = new TemporaryFolder();

    @Before
    public void setup() throws Exception {
        List<PageStoreDir> pageStoreDirs;
        InstancedConfiguration mConf = Configuration.copyGlobal();

        Path mDirPath = mTempFolder.newFolder().toPath();
        InstancedConfiguration dirConf = Configuration.modifiableGlobal();
        dirConf.set(PropertyKey.WORKER_PAGE_STORE_DIRS, ImmutableList.of(mDirPath));
        dirConf.set(PropertyKey.WORKER_PAGE_STORE_SIZES, ImmutableList.of(Constants.MB));
        dirConf.set(PropertyKey.WORKER_PAGE_STORE_TYPE, PageStoreType.LOCAL);
        PageStoreDir pageStoreDir =
                PageStoreDir.createPageStoreDir(
                        new CacheEvictorOptions().setEvictorClass(FIFOCacheEvictor.class),
                        PageStoreOptions.createForWorkerPageStore(dirConf).get(DIR_INDEX));
        mDir = new PagedBlockStoreDir(pageStoreDir, DIR_INDEX);

        try{
            mUfs = new NoopUfsManager();
            conf = Configuration.global();
            mConf.set(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE, mPageSize);
            mConf.set(PropertyKey.WORKER_PAGE_STORE_DIRS, ImmutableList.of(mDirPath));

            // Here mock BlockMasterClientPool and BlockMasterClient since I have no idea about how to override them
            // mockedPool will return a mocked BlockMasterClient when require() is called, and do nothing when releasing, maybe add some action later on
            mBlockMasterClientPool = mock(BlockMasterClientPool.class);
            mockedBlockMasterClient = mock(BlockMasterClient.class);
            when(mBlockMasterClientPool.acquire()).thenReturn(mockedBlockMasterClient);
            doNothing().when(mBlockMasterClientPool).release(any());
            workerId = new AtomicReference<>(-1L);
            cacheManagerOptions = CacheManagerOptions.createForWorker(mConf);
            pageStoreDirs = new ArrayList<PageStoreDir>();
            pageStoreDirs.add(pageStoreDir);
            dirs = PagedBlockStoreDir.fromPageStoreDirs(pageStoreDirs);
        } catch (Exception e) {
            System.out.println(e);
        }

        mListener = mock(BlockStoreEventListener.class);
        doAnswer((i) -> {
            assertEquals(2L, i.getArguments()[0]);
            return 0;
        }).when(mListener).onCommitBlockToLocal(anyLong(), any(BlockStoreLocation.class));
        doAnswer((i) -> {
            assertEquals(2L, i.getArguments()[0]);
            return 0;
        }).when(mListener).onCommitBlockToMaster(anyLong(), any(BlockStoreLocation.class));
    }

    // This Test case success both to commit, no Exception should be throwed, and both onCommit method should be called
    @Test
    public void LocalCommitAndMasterCommitBothSuccess() {
        try {
            pageMetaStore = new PagedBlockMetaStore(dirs) {
                // here commit always success
                @Override
                public PagedBlockMeta commit(long mBlockId) {
                    return super.commit(mBlockId);
                }
            };
            cacheManager = CacheManager.Factory.create(conf, cacheManagerOptions, pageMetaStore);

            pagedBlockStore = new PagedBlockStore(cacheManager, mUfs, mBlockMasterClientPool, workerId, pageMetaStore, cacheManagerOptions.getPageSize());
        } catch (IOException e) {
            throw new RuntimeException();
        }

        prepareBlockStore();

        pagedBlockStore.commitBlock(mSessionId, mBlockId, false);
        verify(mListener).onCommitBlockToLocal(anyLong(), any(BlockStoreLocation.class));
        verify(mListener).onCommitBlockToMaster(anyLong(), any(BlockStoreLocation.class));
    }

    // This Test case success commitToMaster, expecting one exception, and only one onCommit method should be called
    @Test
    public void LocalCommitFailAndMasterCommitSuccess() {
        try {
            pageMetaStore = new PagedBlockMetaStore(dirs) {
                // here commit always throw Exception
                @Override
                public PagedBlockMeta commit(long mBlockId) {
                    throw new RuntimeException();
                }
            };
            cacheManager = CacheManager.Factory.create(conf, cacheManagerOptions, pageMetaStore);

            pagedBlockStore = new PagedBlockStore(cacheManager, mUfs, mBlockMasterClientPool, workerId, pageMetaStore, cacheManagerOptions.getPageSize());
        } catch (IOException e) {
            throw new RuntimeException();
        }

        prepareBlockStore();

        assertThrows(RuntimeException.class, () -> {
            pagedBlockStore.commitBlock(mSessionId, mBlockId, false);
        });

        verify(mListener, never()).onCommitBlockToLocal(anyLong(), any(BlockStoreLocation.class));
        verify(mListener, never()).onCommitBlockToMaster(anyLong(), any(BlockStoreLocation.class));

        try {
            pagedBlockStore.close();
        } catch (Exception e) {
            System.out.println(e);
        }

    }

    // This Test case success commitToLocal, expecting one exception, and only one onCommit method should be called
    @Test
    public void LocalCommitSuccessAndMasterCommitFail() {
        try {
            doAnswer((i) -> {
                throw new AlluxioStatusException(Status.UNAVAILABLE);
            }).when(mockedBlockMasterClient).commitBlock(anyLong(), anyLong(), anyString(), anyString(), anyLong(), anyLong());
        } catch (AlluxioStatusException e) {
            throw new RuntimeException();
        }
        try {
            pageMetaStore = new PagedBlockMetaStore(dirs) {
                // here commit always success
                @Override
                public PagedBlockMeta commit(long mBlockId) {
                    return super.commit(mBlockId);
                }
            };
            cacheManager = CacheManager.Factory.create(conf, cacheManagerOptions, pageMetaStore);

            pagedBlockStore = new PagedBlockStore(cacheManager, mUfs, mBlockMasterClientPool, workerId, pageMetaStore, cacheManagerOptions.getPageSize());
        } catch (IOException e) {
            throw new RuntimeException();
        }

        prepareBlockStore();

        assertThrows(RuntimeException.class, () -> {
            pagedBlockStore.commitBlock(mSessionId, mBlockId, false);
        });
        verify(mListener).onCommitBlockToLocal(anyLong(), any(BlockStoreLocation.class));
        verify(mListener, never()).onCommitBlockToMaster(anyLong(), any(BlockStoreLocation.class));
    }

    // Prepare PageBlockStore and creat a temp block for following test
    public void prepareBlockStore() {
        PagedBlockStoreDir dir =
                (PagedBlockStoreDir) pageMetaStore.allocate(BlockPageId.tempFileIdOf(mBlockId), 1);

        dir.putTempFile(BlockPageId.tempFileIdOf(mBlockId));
        PagedTempBlockMeta blockMeta = new PagedTempBlockMeta(mBlockId, dir);
        pagedBlockStore.createBlock(mSessionId, mBlockId, offset, new CreateBlockOptions(null, null, mBlockSize));
        byte[] data = new byte[mBlockSize];
        Arrays.fill(data, (byte) 1);
        ByteBuffer buf = ByteBuffer.wrap(data);
        try (BlockWriter writer = pagedBlockStore.createBlockWriter(mSessionId, mBlockId)) {
            Thread.sleep(1000);
            writer.append(buf);
        } catch (Exception e) {
            System.out.println(e);
        }

        pagedBlockStore.registerBlockStoreEventListener(mListener);
    }

}
