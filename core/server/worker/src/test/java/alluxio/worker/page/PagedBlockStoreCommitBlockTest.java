package alluxio.worker.page;


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
import alluxio.exception.ExceptionMessage;
import alluxio.exception.PageNotFoundException;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.BlockDoesNotExistRuntimeException;
import alluxio.grpc.ErrorType;
import alluxio.master.NoopUfsManager;
import alluxio.underfs.UfsManager;
//import alluxio.worker.BlockStoreBase;
import alluxio.worker.block.*;
import alluxio.worker.block.io.BlockWriter;
import com.google.common.collect.ImmutableList;
import io.grpc.Status;
import org.apache.logging.log4j.core.tools.picocli.CommandLine;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Spy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import java.nio.file.Path;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class PagedBlockStoreCommitBlockTest {


    // @Spy
    // BlockStoreEventListener listener;
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
    AtomicReference<Long> workerId;

    Boolean mCommitMaster = true;

    private static final int DIR_INDEX = 0;

    private long blockId;

    @Parameterized.Parameters
    public static List<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {2L},
                {3L},
                {4L},
        });
    }

    public PagedBlockStoreCommitBlockTest(long input) {
        this.blockId = input;
    }

    public int mPageSize = 2;

    private static final int offset = 0;

    private PagedBlockStoreDir mDir;

    @Rule
    public TemporaryFolder mTempFolder = new TemporaryFolder();

    @Before
    public void setup() throws Exception {
        CacheManager cacheManager;
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
            BlockMasterClient mockedBlockMasterClient = mock(BlockMasterClient.class);
            when(blockMasterClientPool.acquire()).thenReturn(mockedBlockMasterClient);
            doNothing().when(blockMasterClientPool).release(any());
            doAnswer((i) -> {
                // when testing blockId == 3L, commit to Master should fail, otherwise it will do nothing and success.
                if ((Long)i.getArgument(4) == 3L) {
                    throw new AlluxioRuntimeException(Status.UNAVAILABLE, ExceptionMessage.FAILED_COMMIT_BLOCK_TO_MASTER.getMessage((Long)i.getArgument(5)), new IOException(), ErrorType.Internal, false);
                }
                return null;
            }).when(mockedBlockMasterClient).commitBlock(anyLong(), anyLong(), anyString(), anyString(), anyLong(), anyLong());
            workerId = new AtomicReference<>(-1L);
            cacheManagerOptions = CacheManagerOptions.createForWorker(mConf);
            // pageStoreDirs = PageStoreDir.createPageStoreDirs(cacheManagerOptions);
            pageStoreDirs = new ArrayList<PageStoreDir>();
            pageStoreDirs.add(pageStoreDir);
            dirs = PagedBlockStoreDir.fromPageStoreDirs(pageStoreDirs);
            pageMetaStore = new PagedBlockMetaStore(dirs) {
                @Override
                public PagedBlockMeta commit(long blockId) {
                    if (blockId == 4L) {
                        throw new RuntimeException();
                    }
                    return super.commit(blockId);
                }
            };
            cacheManager = CacheManager.Factory.create(conf, cacheManagerOptions, pageMetaStore);



            // pagedBlockStore = new PagedBlockStore(cacheManager, ufs, blockMasterClientPool, workerId, pageMetaStore, cacheManagerOptions// .getPageSize()) {
            //     @Override
            //     public void commitBlockToMaster(PagedBlockMeta blockMeta) {
            //         if (mCommitMaster) {
            //             // do nothing, 'onCommitToMaster will tell'
            //         } else {
            //             throw new AlluxioRuntimeException(Status.UNAVAILABLE, ExceptionMessage.FAILED_COMMIT_BLOCK_TO_MASTER.getMessage// (blockId), new IOException(), ErrorType.Internal, false);
            //         }
            //     }
            // };
            pagedBlockStore = new PagedBlockStore(cacheManager, ufs, blockMasterClientPool, workerId, pageMetaStore, cacheManagerOptions .getPageSize());
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    @Test
    public void LocalCommitAndMasterCommit() {
        // listener = new AbstractBlockStoreEventListener() {
        //     @Override
        //     public void onCommitBlockToLocal(long blockId, BlockStoreLocation location) {
        //         assertEquals(2L, blockId);
        //         // assertEquals(dirs.get(0).getLocation(), location);
        //     }

        //     @Override
        //     public void onCommitBlockToMaster(long blockId, BlockStoreLocation location) {
        //         assertEquals(2L, blockId);
        //         // assertEquals(dirs.get(0).getLocation(), location);
        //     }
        // };
        System.out.println("finding null pointer " + pagedBlockStore);
        PagedBlockStoreDir dir =
                (PagedBlockStoreDir) pageMetaStore.allocate(BlockPageId.tempFileIdOf(blockId), 1);

        dir.putTempFile(BlockPageId.tempFileIdOf(blockId));
        PagedTempBlockMeta blockMeta = new PagedTempBlockMeta(blockId, dir);
        // pageMetaStore.addTempBlock(blockMeta);
        pagedBlockStore.createBlock(1L, blockId, offset, new CreateBlockOptions(null, null, 64));
        byte[] data = new byte[64];
        Arrays.fill(data, (byte) 1);
        ByteBuffer buf = ByteBuffer.wrap(data);
        try (BlockWriter writer = pagedBlockStore.createBlockWriter(1L, blockId)) {
            writer.append(buf);
        } catch (Exception e) {
            System.out.println("writer failed");
        }

        pagedBlockStore.registerBlockStoreEventListener(listener);
        pagedBlockStore.commitBlock(1L, blockId, false);
        verify(listener).onCommitBlockToLocal(anyLong(), any(BlockStoreLocation.class));
        verify(listener).onCommitBlockToMaster(anyLong(), any(BlockStoreLocation.class));
    }
}
