/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file.cache.store;

import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageInfo;
import alluxio.client.file.cache.PageStore;
import alluxio.exception.PageNotFoundException;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The {@link LocalPageStore} is an implementation of {@link PageStore} which
 * stores all pages in a directory somewhere on the local disk.
 */
@NotThreadSafe
public class LocalPageStore implements PageStore {
  private static final Logger LOG = LoggerFactory.getLogger(LocalPageStore.class);

  private final String mRoot;
  private final AtomicLong mSize = new AtomicLong(0);
  private final AtomicLong mBytes = new AtomicLong(0);
  private final long mPageSize;
  private final Pattern mPagePattern;

  /**
   * Creates a new instance of {@link LocalPageStore}.
   *
   * @param options options for the local page store
   * @throws IOException when fails to create a {@link LocalPageStore}
   */
  public LocalPageStore(LocalPageStoreOptions options) throws IOException {
    mRoot = options.getRootDir();
    mPageSize = options.getPageSize();
    Path rootDir = Paths.get(mRoot);
    mPagePattern = Pattern.compile(
        String.format("%s/%d/(\\d+)/(\\d+)", Pattern.quote(rootDir.toString()), mPageSize));
    try {
      boolean invalidPage = Files.exists(rootDir) && Files.walk(rootDir)
          .filter(Files::isRegularFile)
          .anyMatch(path -> {
            if (getPageId(path) == null) {
              LOG.warn("Invalid page path {}", path);
              return true;
            }
            try {
              mBytes.getAndAdd(Files.size(path));
            } catch (IOException e) {
              LOG.warn("Fail to get file size {}", e.toString());
            }
            mSize.incrementAndGet();
            return false;
          });
      if (invalidPage || mBytes.get() > options.getCacheSize()) {
        LOG.warn("Cannot recover from cached data: {}",
            invalidPage ? "Invalid page file found" : "Cached data size exceeded configured value");
        FileUtils.cleanDirectory(new File(mRoot));
        mSize.set(0);
        mBytes.set(0);
      }
    } catch (IOException e) {
      throw new IOException(String.format("can't initialize page store at %s", mRoot), e);
    }
  }

  @Override
  public void put(PageId pageId, byte[] page) throws IOException {
    Path p = getFilePath(pageId);
    if (!Files.exists(p)) {
      Path parent = Preconditions.checkNotNull(p.getParent(),
          "parent of cache file should not be null");
      Files.createDirectories(parent);
      Files.createFile(p);
    }
    try (FileOutputStream fos = new FileOutputStream(p.toFile(), false)) {
      fos.write(page);
    }
    mSize.incrementAndGet();
    mBytes.getAndAdd(page.length);
  }

  @Override
  public ReadableByteChannel get(PageId pageId, int pageOffset)
      throws IOException, PageNotFoundException {
    Preconditions.checkArgument(pageOffset >= 0, "page offset should be non-negative");
    Path p = getFilePath(pageId);
    if (!Files.exists(p)) {
      throw new PageNotFoundException(p.toString());
    }
    File f = p.toFile();
    Preconditions.checkArgument(pageOffset <= f.length(),
        "page offset %s exceeded page size %s", pageOffset, f.length());
    FileInputStream fis = new FileInputStream(p.toFile());
    try {
      fis.skip(pageOffset);
      return fis.getChannel();
    } catch (Throwable t) {
      fis.close();
      throw t;
    }
  }

  @Override
  public void delete(PageId pageId, long pageSize) throws IOException, PageNotFoundException {
    Path p = getFilePath(pageId);
    if (!Files.exists(p)) {
      throw new PageNotFoundException(p.toString());
    }
    Files.delete(p);
    mSize.decrementAndGet();
    mBytes.getAndAdd(-pageSize);
    Path parent = Preconditions.checkNotNull(p.getParent(),
        "parent of cache file should not be null");
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(parent)) {
      if (!stream.iterator().hasNext()) {
        Files.delete(parent);
      }
    }
  }

  private Path getFilePath(PageId pageId) {
    return Paths.get(mRoot, Long.toString(mPageSize), Long.toString(pageId.getFileId()),
        Long.toString(pageId.getPageIndex()));
  }

  /**
   * @param path path of a file
   * @return the corresponding page id, or null if the file name does not match the pattern
   */
  @Nullable
  private PageId getPageId(Path path) {
    Matcher matcher = mPagePattern.matcher(path.toString());
    if (!matcher.matches()) {
      return null;
    }
    try {
      String parentName = Preconditions.checkNotNull(matcher.group(1));
      String fileName = Preconditions.checkNotNull(matcher.group(2));
      long fileId = Long.parseLong(parentName);
      long pageIndex = Long.parseLong(fileName);
      return new PageId(fileId, pageIndex);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /**
   * @param path path of a file
   * @return the corresponding page info for the file otherwise null
   */
  @Nullable
  private PageInfo getPageInfo(Path path) {
    PageId pageId = getPageId(path);
    long pageSize;
    if (pageId == null) {
      return null;
    }
    try {
      pageSize = Files.size(path);
    } catch (IOException e) {
      LOG.error("Failed to get file size for " + path, e);
      return null;
    }
    return new PageInfo(pageId, pageSize);
  }

  @Override
  public void close() {
    // no-op
  }

  @Override
  public long pages() {
    return mSize.get();
  }

  @Override
  public long bytes() {
    return mBytes.get();
  }

  @Override
  public Collection<PageInfo> getPages() throws IOException {
    Path rootDir = Paths.get(mRoot);
    if (!Files.exists(rootDir)) {
      return Collections.emptyList();
    }
    try (Stream<Path> stream = Files.walk(rootDir)) {
      return stream
          .filter(Files::isRegularFile)
          .map(this::getPageInfo)
          .filter(Objects::nonNull)
          .collect(Collectors.toList());
    }
  }
}
