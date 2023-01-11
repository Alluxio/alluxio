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
    BlockStoreEventListener listener0 = new AbstractBlockStoreEventListener() {
        @Override
        public void onCommitBlockToLocal(long blockId, BlockStoreLocation location) {
            assertEquals(2L, blockId);
            // assertEquals(dirs.get(0).getLocation(), location);
        }

        @Override
        public void onCommitBlockToMaster(long blockId, BlockStoreLocation location) {
            assertEquals(2L, blockId);
            // assertEquals(dirs.get(0).getLocation(), location);
        }
    };

    BlockStoreEventListener listener = spy(listener0);
    UfsManager ufs;
    AlluxioConfiguration conf;
    CacheManagerOptions cacheManagerOptions;
    PagedBlockMetaStore pageMetaStore;
    List<PagedBlockStoreDir> dirs;
    PagedBlockStore pagedBlockStore;
    BlockMasterClientPool blockMasterClientPool;
    BlockMasterClient mockedBlockMasterClient;
    AtomicReference<Long> workerId;

    CacheManager cacheManager;

    private static final int DIR_INDEX = 0;

    private static long blockId = 2L;

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
            ufs = new NoopUfsManager();
            conf = Configuration.global();
            mConf.set(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE, mPageSize);
            mConf.set(PropertyKey.WORKER_PAGE_STORE_DIRS, ImmutableList.of(mDirPath));

            // Here mock BlockMasterClientPool and BlockMasterClient since I have no idea about how to override them
            // mockedPool will return a mocked BlockMasterClient when require() is called, and do nothing when releasing, maybe add some action later on
            blockMasterClientPool = mock(BlockMasterClientPool.class);
            mockedBlockMasterClient = mock(BlockMasterClient.class);
            when(blockMasterClientPool.acquire()).thenReturn(mockedBlockMasterClient);
            doNothing().when(blockMasterClientPool).release(any());
            workerId = new AtomicReference<>(-1L);
            cacheManagerOptions = CacheManagerOptions.createForWorker(mConf);
            pageStoreDirs = new ArrayList<PageStoreDir>();
            pageStoreDirs.add(pageStoreDir);
            dirs = PagedBlockStoreDir.fromPageStoreDirs(pageStoreDirs);
        } catch (Exception e) {
            System.out.println(e);
        }
    }


    // This Test case success both to commit, no Exception should be throwed, and both onCommit method should be called
    @Test
    public void LocalCommitAndMasterCommitBothSuccess() {
        try {
            pageMetaStore = new PagedBlockMetaStore(dirs) {
                // here commit always success
                @Override
                public PagedBlockMeta commit(long blockId) {
                    return super.commit(blockId);
                }
            };
            cacheManager = CacheManager.Factory.create(conf, cacheManagerOptions, pageMetaStore);

            pagedBlockStore = new PagedBlockStore(cacheManager, ufs, blockMasterClientPool, workerId, pageMetaStore, cacheManagerOptions.getPageSize());
        } catch (IOException e) {
            throw new RuntimeException();
        }

        PagedBlockStoreDir dir =
                (PagedBlockStoreDir) pageMetaStore.allocate(BlockPageId.tempFileIdOf(blockId), 1);

        dir.putTempFile(BlockPageId.tempFileIdOf(blockId));
        PagedTempBlockMeta blockMeta = new PagedTempBlockMeta(blockId, dir);
        pagedBlockStore.createBlock(1L, blockId, offset, new CreateBlockOptions(null, null, 64));
        byte[] data = new byte[64];
        Arrays.fill(data, (byte) 1);
        ByteBuffer buf = ByteBuffer.wrap(data);
        try (BlockWriter writer = pagedBlockStore.createBlockWriter(1L, blockId)) {
            // LocalCacheManager line 107 🤔
            Thread.sleep(1000);
            writer.append(buf);
        } catch (Exception e) {
            System.out.println(e);
        }

        pagedBlockStore.registerBlockStoreEventListener(listener);
        pagedBlockStore.commitBlock(1L, blockId, false);
        verify(listener).onCommitBlockToLocal(anyLong(), any(BlockStoreLocation.class));
        verify(listener).onCommitBlockToMaster(anyLong(), any(BlockStoreLocation.class));
    }

    // This Test case success commitToMaster, expecting one exception, and only one onCommit method should be called
    @Test
    public void LocalCommitFailAndMasterCommitSuccess() {
        try {
            pageMetaStore = new PagedBlockMetaStore(dirs) {
                // here commit always throw Exception
                @Override
                public PagedBlockMeta commit(long blockId) {
                    throw new RuntimeException();
                }
            };
            cacheManager = CacheManager.Factory.create(conf, cacheManagerOptions, pageMetaStore);

            pagedBlockStore = new PagedBlockStore(cacheManager, ufs, blockMasterClientPool, workerId, pageMetaStore, cacheManagerOptions.getPageSize());
        } catch (IOException e) {
            throw new RuntimeException();
        }
        PagedBlockStoreDir dir =
                (PagedBlockStoreDir) pageMetaStore.allocate(BlockPageId.tempFileIdOf(blockId), 1);

        dir.putTempFile(BlockPageId.tempFileIdOf(blockId));
        PagedTempBlockMeta blockMeta = new PagedTempBlockMeta(blockId, dir);
        pagedBlockStore.createBlock(1L, blockId, offset, new CreateBlockOptions(null, null, 64));
        byte[] data = new byte[64];
        Arrays.fill(data, (byte) 1);
        ByteBuffer buf = ByteBuffer.wrap(data);
        try (BlockWriter writer = pagedBlockStore.createBlockWriter(1L, blockId)) {
            writer.append(buf);
        } catch (Exception e) {
        }

        pagedBlockStore.registerBlockStoreEventListener(listener);
        assertThrows(RuntimeException.class, () -> {
            pagedBlockStore.commitBlock(1L, blockId, false);
        });

        verify(listener, never()).onCommitBlockToLocal(anyLong(), any(BlockStoreLocation.class));
        verify(listener, never()).onCommitBlockToMaster(anyLong(), any(BlockStoreLocation.class));

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
                public PagedBlockMeta commit(long blockId) {
                    return super.commit(blockId);
                }
            };
            cacheManager = CacheManager.Factory.create(conf, cacheManagerOptions, pageMetaStore);

            pagedBlockStore = new PagedBlockStore(cacheManager, ufs, blockMasterClientPool, workerId, pageMetaStore, cacheManagerOptions.getPageSize());
        } catch (IOException e) {
            throw new RuntimeException();
        }
        PagedBlockStoreDir dir =
                (PagedBlockStoreDir) pageMetaStore.allocate(BlockPageId.tempFileIdOf(blockId), 1);

        dir.putTempFile(BlockPageId.tempFileIdOf(blockId));
        PagedTempBlockMeta blockMeta = new PagedTempBlockMeta(blockId, dir);
        pagedBlockStore.createBlock(1L, blockId, offset, new CreateBlockOptions(null, null, 64));
        byte[] data = new byte[64];
        Arrays.fill(data, (byte) 1);
        ByteBuffer buf = ByteBuffer.wrap(data);
        try (BlockWriter writer = pagedBlockStore.createBlockWriter(1L, blockId)) {
            Thread.sleep(1000);
            writer.append(buf);
        } catch (Exception e) {
            System.out.println("writer failed");
        }

        pagedBlockStore.registerBlockStoreEventListener(listener);
        assertThrows(RuntimeException.class, () -> {
            pagedBlockStore.commitBlock(1L, blockId, false);
        });
        verify(listener).onCommitBlockToLocal(anyLong(), any(BlockStoreLocation.class));
        verify(listener, never()).onCommitBlockToMaster(anyLong(), any(BlockStoreLocation.class));
    }

}
