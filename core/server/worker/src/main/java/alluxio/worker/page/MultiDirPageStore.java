package alluxio.worker.page;

import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageInfo;
import alluxio.client.file.cache.PageStore;
import alluxio.exception.PageNotFoundException;
import alluxio.exception.status.ResourceExhaustedException;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

/**
 *
 */
public class MultiDirPageStore implements PageStore {

  private final List<PagedStorageDir> dirs;

  /**
   *
   * @param dirs
   */
  public MultiDirPageStore(List<PagedStorageDir> dirs) {
    this.dirs = dirs;
  }

  @Override
  public void put(PageId pageId, byte[] page) throws ResourceExhaustedException, IOException {

  }

  @Override
  public int get(PageId pageId, int pageOffset, int bytesToRead, byte[] buffer, int bufferOffset)
      throws IOException, PageNotFoundException {
    return 0;
  }

  @Override
  public void delete(PageId pageId) throws IOException, PageNotFoundException {

  }

  @Override
  public Stream<PageInfo> getPages() throws IOException {
    return null;
  }

  @Override
  public long getCacheSize() {
    return 0;
  }

  @Override
  public void close() throws Exception {

  }
}
