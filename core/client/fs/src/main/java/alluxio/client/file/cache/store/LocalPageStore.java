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
import java.util.concurrent.atomic.AtomicInteger;
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
  private final AtomicInteger mSize = new AtomicInteger(0);
  private final long mPageSize;

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
    try {
      boolean invalidPage = Files.exists(rootDir) && Files.walk(rootDir)
          .filter(Files::isRegularFile)
          .anyMatch(path -> {
            if (getPageId(path) == null) {
              return true;
            }
            mSize.incrementAndGet();
            return false;
          });
      if (invalidPage || (long) mSize.get() * mPageSize > options.getCacheSize()) {
        FileUtils.cleanDirectory(new File(mRoot));
        mSize.set(0);
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
  public void delete(PageId pageId) throws IOException, PageNotFoundException {
    Path p = getFilePath(pageId);
    if (!Files.exists(p)) {
      throw new PageNotFoundException(p.toString());
    }
    Files.delete(p);
    mSize.decrementAndGet();
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
    Path parent = path.getParent();
    if (parent == null) {
      return null;
    }
    Path grandparent = parent.getParent();
    if (grandparent == null) {
      return null;
    }
    if (!Paths.get(mRoot).equals(grandparent.getParent())) {
      return null;
    }
    try {
      Path fileName = path.getFileName();
      Path parentName = parent.getFileName();
      Path grandparentName = grandparent.getFileName();
      if (fileName == null || parentName == null || grandparentName == null
          || !Long.toString(mPageSize).equals(grandparentName.toString())) {
        return null;
      }
      long pageIndex = Long.parseLong(fileName.toString());
      long fileId = Long.parseLong(parentName.toString());
      return new PageId(fileId, pageIndex);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  @Override
  public void close() {
    // no-op
  }

  @Override
  public int size() {
    return mSize.get();
  }

  @Override
  public Collection<PageId> getPages() throws IOException {
    Path rootDir = Paths.get(mRoot);
    if (!Files.exists(rootDir)) {
      return Collections.emptyList();
    }
    try (Stream<Path> stream = Files.walk(rootDir)) {
      return stream
          .filter(Files::isRegularFile)
          .map(this::getPageId)
          .filter(Objects::nonNull)
          .collect(Collectors.toList());
    }
  }
}
