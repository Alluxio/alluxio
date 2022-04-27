package alluxio.worker.page;

import alluxio.client.file.cache.DefaultMetaStore;
import alluxio.client.file.cache.MetaStore;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageInfo;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.PageNotFoundException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PagedBlockMetaStore extends DefaultMetaStore {

  private Map<Long, Set<Long>> mBlockPageMap = new HashMap<>();

  public PagedBlockMetaStore(AlluxioConfiguration conf) {
    super(conf);
  }


  @Override
  public void addPage(PageId pageId, PageInfo pageInfo) {
    super.addPage(pageId, pageInfo);
    long blockId = Long.parseLong(pageId.getFileId());
    mBlockPageMap
        .computeIfAbsent(blockId, k -> new HashSet<>())
        .add(pageId.getPageIndex());
  }

  @Override
  public PageInfo removePage(PageId pageId) throws PageNotFoundException {
    PageInfo pageInfo = super.removePage(pageId);
    long blockId = Long.parseLong(pageId.getFileId());
    mBlockPageMap.computeIfPresent(blockId, (k, pageIndexes) -> {
      pageIndexes.remove(pageId.getPageIndex());
      if (pageIndexes.isEmpty()) {
        return null;
      } else {
        return pageIndexes;
      }
    });
    return pageInfo;
  }
}
