package alluxio.worker.page;

import alluxio.AlluxioTestDirectory;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.CacheManagerOptions;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.store.PageStoreDir;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.NoopUfsManager;
import alluxio.underfs.UfsManager;
//import alluxio.worker.BlockStoreBase;
import alluxio.worker.block.AbstractBlockStoreEventListener;
import alluxio.worker.block.BlockMasterClientPool;
import alluxio.worker.block.BlockStoreEventListener;
import alluxio.worker.block.BlockStoreLocation;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

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

    @Before
    public void setup() throws Exception {
        CacheManager cacheManager;
        List<PageStoreDir> pageStoreDirs;
        try{
            ufs = new NoopUfsManager();
            conf = Configuration.global();
            blockMasterClientPool = new BlockMasterClientPool();
            workerId = new AtomicReference<>(-1L);
            pageStoreDirs = PageStoreDir.createPageStoreDirs(cacheManagerOptions);
            dirs = PagedBlockStoreDir.fromPageStoreDirs(pageStoreDirs);
            pageMetaStore = new PagedBlockMetaStore(dirs);
            cacheManager = CacheManager.Factory.create(conf, cacheManagerOptions, pageMetaStore);
            cacheManagerOptions = CacheManagerOptions.createForWorker(conf);
            pagedBlockStore = new PagedBlockStore(cacheManager, ufs, blockMasterClientPool, workerId, pageMetaStore, cacheManagerOptions.getPageSize());
        } catch (Exception e) {

        }
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

        pagedBlockStore.registerBlockStoreEventListener(listener);
        pagedBlockStore.commitBlock(1, 1, true);
    }
}
