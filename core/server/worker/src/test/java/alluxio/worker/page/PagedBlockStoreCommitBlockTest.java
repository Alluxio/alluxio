package alluxio.worker.page;

import alluxio.AlluxioTestDirectory;
import alluxio.Constants;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.CacheManagerOptions;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.evictor.CacheEvictorOptions;
import alluxio.client.file.cache.evictor.FIFOCacheEvictor;
import alluxio.client.file.cache.store.PageStoreDir;
import alluxio.client.file.cache.store.PageStoreOptions;
import alluxio.client.file.cache.store.PageStoreType;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.NoopUfsManager;
import alluxio.underfs.UfsManager;
//import alluxio.worker.BlockStoreBase;
import alluxio.worker.block.AbstractBlockStoreEventListener;
import alluxio.worker.block.BlockMasterClientPool;
import alluxio.worker.block.BlockStoreEventListener;
import alluxio.worker.block.BlockStoreLocation;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import java.nio.file.Path;

import static org.junit.Assert.assertEquals;

public class PagedBlockStoreCommitBlockTest {
    UfsManager ufs;
    AlluxioConfiguration conf;
    CacheManagerOptions cacheManagerOptions;
    PagedBlockMetaStore pageMetaStore;
    List<PagedBlockStoreDir> dirs;
    PagedBlockStore pagedBlockStore;
    BlockMasterClientPool blockMasterClientPool;
    AtomicReference<Long> workerId;

    private static final int DIR_INDEX = 0;
    private static final long blockId = 2L;

    private PagedBlockStoreDir mDir;

    @Rule
    public TemporaryFolder mTempFolder = new TemporaryFolder();

    @Before
    public void setup() throws Exception {
        CacheManager cacheManager;
        List<PageStoreDir> pageStoreDirs;

        Path mDirPath = mTempFolder.newFolder().toPath();
        System.out.println(mDirPath);
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
            blockMasterClientPool = new BlockMasterClientPool();
            workerId = new AtomicReference<>(-1L);
            cacheManagerOptions = CacheManagerOptions.createForWorker(conf);
            // pageStoreDirs = PageStoreDir.createPageStoreDirs(cacheManagerOptions);
            pageStoreDirs = new ArrayList<PageStoreDir>();
            pageStoreDirs.add(pageStoreDir);
            dirs = PagedBlockStoreDir.fromPageStoreDirs(pageStoreDirs);
            pageMetaStore = new PagedBlockMetaStore(dirs);
            cacheManager = CacheManager.Factory.create(conf, cacheManagerOptions, pageMetaStore);
            pagedBlockStore = new PagedBlockStore(cacheManager, ufs, blockMasterClientPool, workerId, pageMetaStore, cacheManagerOptions.getPageSize());
        } catch (Exception e) {
            System.out.println(e);
        }
        System.out.println("Setup finished");
    }

    @Test
    public void LocalCommitAndMasterCommit() {
        BlockStoreEventListener listener = new AbstractBlockStoreEventListener() {
            @Override
            public void onCommitBlockToLocal(long blockId, BlockStoreLocation location) {
                assertEquals(1, blockId);
                assertEquals(dirs.get(0).getLocation(), location);
            }

            @Override
            public void onCommitBlockToMaster(long blockId, BlockStoreLocation location) {
                assertEquals(1, blockId);
                assertEquals(dirs.get(0).getLocation(), location);
            }
        };
        PagedBlockStoreDir dir =
                (PagedBlockStoreDir) pageMetaStore.allocate(BlockPageId.tempFileIdOf(blockId), 0);

        dir.putTempFile(BlockPageId.tempFileIdOf(blockId));
        PagedTempBlockMeta blockMeta = new PagedTempBlockMeta(blockId, dir);
        pageMetaStore.addTempBlock(blockMeta);

        pagedBlockStore.registerBlockStoreEventListener(listener);
        pagedBlockStore.commitBlock(1L, 2L, false);
    }
}
