package alluxio.client.file.cache.store;

import alluxio.cachestore.LibRawDeviceStore;
import alluxio.cachestore.RawDeviceStore;
import alluxio.client.file.cache.CacheUsage;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageInfo;
import alluxio.client.file.cache.PageStore;
import alluxio.client.file.cache.evictor.CacheEvictor;
import alluxio.client.quota.CacheScope;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.function.Consumer;

public class RawDevicePageStoreDir extends QuotaManagedPageStoreDir {

  private final RawDeviceStore mRawDeviceStore;
  public RawDevicePageStoreDir(PageStoreOptions pageStoreOptions,
                               PageStore pageStore,
                               CacheEvictor evictor) {
    super(pageStoreOptions.getRootDir(),
        (long) (pageStoreOptions.getCacheSize() / (1 + pageStoreOptions.getOverheadRatio())),
        evictor);
    mRawDeviceStore = (RawDeviceStore) pageStore;
  }

  @Override
  public Optional<CacheUsage> getUsage() {

    return Optional.of(new QuotaManagedPageStoreDir.Usage());
  }

  @Override
  public PageStore getPageStore() {
    return mRawDeviceStore;
  }

  @Override
  public void reset() throws IOException {
    // noop
  }

  @Override
  public Path getRootPath() {
    return Paths.get("/");
  }

  @Override
  public void scanPages(Consumer<Optional<PageInfo>> pageInfoConsumer) throws IOException {
    String startFileId = "";
    long pageIdx = 0;
    LibRawDeviceStore.JniPageInfo [] jniPageInfos = mRawDeviceStore
        .listPages(new PageId(startFileId, pageIdx), 100);
    while (jniPageInfos != null) {
      for (LibRawDeviceStore.JniPageInfo jniPageInfo : jniPageInfos) {
        startFileId = jniPageInfo.getFileId();
        pageIdx = jniPageInfo.getPageId();
        PageInfo pageInfo = new PageInfo(
            new PageId(startFileId, pageIdx),
            jniPageInfo.getFileSize(), CacheScope.GLOBAL, this,
            jniPageInfo.getCreationTime());
        pageInfoConsumer.accept(Optional.of(pageInfo));
      }

      jniPageInfos = mRawDeviceStore
          .listPages(new PageId(startFileId, pageIdx + 1), 100);
    }
  }
}
