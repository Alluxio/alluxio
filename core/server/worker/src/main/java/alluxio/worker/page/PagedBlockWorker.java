package alluxio.worker.page;

import static java.util.Objects.requireNonNull;

import alluxio.Constants;
import alluxio.Server;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.store.PageStoreDir;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.grpc.AsyncCacheRequest;
import alluxio.grpc.CacheRequest;
import alluxio.grpc.GetConfigurationPOptions;
import alluxio.grpc.GrpcService;
import alluxio.grpc.ServiceType;
import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.UfsManager;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.wire.Configuration;
import alluxio.wire.FileInfo;
import alluxio.worker.AbstractWorker;
import alluxio.worker.block.BlockHeartbeatReport;
import alluxio.worker.block.BlockStoreMeta;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.CreateBlockOptions;
import alluxio.worker.block.UfsInputStreamCache;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class PagedBlockWorker extends AbstractWorker implements BlockWorker {
  private final PagedBlockMetaStore mPagedBlockMetaStore;
  private final CacheManager mCacheManager;
  private final AlluxioConfiguration mConf;
  private final List<PageStoreDir> mPageStoreDirs;
  private final UfsManager mUfsManager;
  private final UfsInputStreamCache mUfsInStreamCache = new UfsInputStreamCache();

  /**
   * Constructor for PagedBlockWorker.
   * @param ufsManager
   * @param pagedBlockMetaStore
   * @param cacheManager
   * @param pageStoreDirs
   * @param conf
   */
  public PagedBlockWorker(UfsManager ufsManager,
                          PagedBlockMetaStore pagedBlockMetaStore,
                          CacheManager cacheManager,
                          List<PageStoreDir> pageStoreDirs,
                          AlluxioConfiguration conf) {
    super(ExecutorServiceFactories.fixedThreadPool("paged-block-worker-executor", 5));
    mUfsManager = requireNonNull(ufsManager);
    mPagedBlockMetaStore = requireNonNull(pagedBlockMetaStore);
    mCacheManager = requireNonNull(cacheManager);
    mPageStoreDirs = pageStoreDirs;
    mConf = requireNonNull(conf);
  }

  @Override
  public Set<Class<? extends Server>> getDependencies() {
    return null;
  }

  @Override
  public String getName() {
    return Constants.BLOCK_WORKER_NAME;
  }

  @Override
  public Map<ServiceType, GrpcService> getServices() {
    return null;
  }

  @Override
  public void cleanupSession(long sessionId) {
  }

  @Override
  public AtomicReference<Long> getWorkerId() {
    return null;
  }

  @Override
  public void abortBlock(long sessionId, long blockId) throws IOException {

  }

  @Override
  public void commitBlock(long sessionId, long blockId, boolean pinOnCreate) throws IOException {

  }

  @Override
  public void commitBlockInUfs(long blockId, long length) throws IOException {

  }

  @Override
  public String createBlock(long sessionId, long blockId, int tier,
                            CreateBlockOptions createBlockOptions)
      throws WorkerOutOfSpaceException, IOException {
    //TODO(Beinan): port the allocator algorithm from tiered block store
    PageStoreDir pageStoreDir = mPageStoreDirs.get(
        Math.floorMod(Long.hashCode(blockId), mPageStoreDirs.size()));
    pageStoreDir.addFileToDir(String.valueOf(blockId));
    return "DUMMY_FILE_PATH";
  }

  @Override
  public BlockWriter createBlockWriter(long sessionId, long blockId) throws IOException {
    return new PagedBlockWriter(mCacheManager, blockId, mConf);
  }

  @Override
  public BlockHeartbeatReport getReport() {
    return null;
  }

  @Override
  public BlockStoreMeta getStoreMeta() {
    return null;
  }

  @Override
  public BlockStoreMeta getStoreMetaFull() {
    return null;
  }

  @Override
  public boolean hasBlockMeta(long blockId) {
    for (PageStoreDir pageStoreDir : mPageStoreDirs) {
      if (pageStoreDir.hasFile(String.valueOf(blockId))) {
        return true;
      }
    }
    return false;
  }

  @Override
  public BlockReader createBlockReader(long sessionId, long blockId, long offset,
                                       boolean positionShort, Protocol.OpenUfsBlockOptions options)
      throws BlockDoesNotExistException, IOException {
    return new PagedBlockReader(mCacheManager, mUfsManager, mUfsInStreamCache, mConf, blockId,
        options);
  }

  @Override
  public BlockReader createUfsBlockReader(long sessionId, long blockId, long offset,
                                          boolean positionShort,
                                          Protocol.OpenUfsBlockOptions options) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removeBlock(long sessionId, long blockId) throws IOException {

  }

  @Override
  public void requestSpace(long sessionId, long blockId, long additionalBytes)
      throws WorkerOutOfSpaceException, IOException {

  }

  @Override
  public void asyncCache(AsyncCacheRequest request) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void cache(CacheRequest request) throws AlluxioException, IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updatePinList(Set<Long> pinnedInodes) {

  }

  @Override
  public FileInfo getFileInfo(long fileId) throws IOException {
    return null;
  }

  @Override
  public void clearMetrics() {

  }

  @Override
  public Configuration getConfiguration(GetConfigurationPOptions options) {
    return null;
  }

  @Override
  public List<String> getWhiteList() {
    return null;
  }
}
