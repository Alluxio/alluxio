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

package alluxio.worker.page;

import alluxio.client.file.cache.PageId;

import com.google.common.base.Preconditions;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Specialized {@link PageId} when it's part of a block.
 */
public final class BlockPageId extends PageId {
  // the name pattern of the page files of a block
  // the block id and size are encoded as 16-byte hexadecimal
  private static final Pattern FILE_ID_PATTERN =
      Pattern.compile("paged_block_([0-9a-fA-F]{16})_size_([0-9a-fA-F]{16})");
  private static final String FILE_ID_TEMPLATE = "paged_block_%016x_size_%016x";
  // placeholder for temp blocks
  private static final long INVALID_BLOCK_SIZE = -1;

  /**
   * this is constructed from {@link PageId#getFileId()} and cached to avoid parsing the string
   * multiple times.
   */
  private final long mBlockId;

  /**
   * The block size. It is here because we need to put this information into the page store
   * but it does not currently support page metadata.
   * Todo(bowen): make this part of page metadata and remove it
   */
  private final long mBlockSize;

  /**
   * @param blockId string representation of the block ID in base 10
   * @param pageIndex index of the page in the block
   * @param blockSize block size
   * @throws NumberFormatException when {@code blockId} cannot be parsed as a {@code long}
   */
  public BlockPageId(String blockId, long pageIndex, long blockSize) {
    this(Long.parseLong(blockId), pageIndex, blockSize);
  }

  /**
   * Creates an instance with a block ID as a {@code long}.
   * @param blockId the block ID
   * @param pageIndex index of the page in the block
   * @param blockSize block size
   */
  public BlockPageId(long blockId, long pageIndex, long blockSize) {
    super(fileIdOf(blockId, blockSize), pageIndex);
    mBlockId = blockId;
    mBlockSize = blockSize;
  }

  /**
   * Creates a new page of the temporary block.
   * @param blockId
   * @param pageIndex
   * @return page ID
   */
  public static BlockPageId newTempPage(long blockId, long pageIndex) {
    return new BlockPageId(blockId, pageIndex, INVALID_BLOCK_SIZE);
  }

  /**
   * @param blockId
   * @param blockSize
   * @return file ID
   */
  public static String fileIdOf(long blockId, long blockSize) {
    return String.format(FILE_ID_TEMPLATE, blockId, blockSize).intern();
  }

  /**
   * @param blockId
   * @return file ID
   */
  public static String tempFileIdOf(long blockId) {
    return fileIdOf(blockId, INVALID_BLOCK_SIZE);
  }

  /**
   * @param fileId
   * @return block ID
   * @throws IllegalArgumentException when the fileId does not contain a valid block ID
   */
  public static long parseBlockId(String fileId) {
    Matcher matcher = FILE_ID_PATTERN.matcher(fileId);
    if (matcher.matches()) {
      try {
        // block id can be negative, Long.parseLong will throw in this case
        return Long.parseUnsignedLong(matcher.group(1), 16);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            String.format("fileId %s does not contain a valid block ID", fileId), e);
      }
    }
    throw new IllegalArgumentException(
        String.format("fileId %s is not a valid paged block ID", fileId));
  }

  /**
   * @param fileId
   * @return block size
   * @throws IllegalArgumentException when the fileId does not contain a valid block size
   */
  public static long parseBlockSize(String fileId) {
    Matcher matcher = FILE_ID_PATTERN.matcher(fileId);
    if (matcher.matches()) {
      try {
        String blockSizeString = matcher.group(2);
        long size = Long.parseLong(blockSizeString, 16);
        Preconditions.checkArgument(size >= 0, "negative block size: %s", blockSizeString);
        return size;
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            String.format("fileId %s does not contain a valid block size", fileId), e);
      }
    }
    throw new IllegalArgumentException(
        String.format("fileId %s is not a valid paged block ID", fileId));
  }

  /**
   * @return the block ID
   */
  public long getBlockId() {
    return mBlockId;
  }

  /**
   * @return the block size
   */
  public long getBlockSize() {
    return mBlockSize;
  }

  /**
   * Try to downcast a {@link PageId} to a {@link BlockPageId}. If the object is already a block
   * page ID, then it is immediately returned. Otherwise, attempt to parse the {@code fileId} of
   * the page if the encoded file name matches a block and contains all necessary metadata.
   *
   * @param pageId the page ID to downcast
   * @return the downcast block page ID
   * @throws IllegalArgumentException if the page ID cannot be cast to a block page ID
   */
  public static BlockPageId downcast(PageId pageId) {
    if (pageId instanceof BlockPageId) {
      return (BlockPageId) pageId;
    }
    String fileId = pageId.getFileId();
    long blockId = parseBlockId(fileId);
    long blockSize = parseBlockSize(fileId);
    return new BlockPageId(blockId, pageId.getPageIndex(), blockSize);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PageId)) {
      return false;
    }
    // a fast path comparing longs instead of strings when both are BlockPageIds
    if (o instanceof BlockPageId) { // we are final so instanceof check is ok
      BlockPageId that = (BlockPageId) o;
      // block size is metadata so shouldn't be considered for equality
      return mBlockId == that.mBlockId && getPageIndex() == that.getPageIndex();
    }
    // otherwise o is either the super class PageId or some other subclass of PageId.
    // super.equals(o) does not work here because if o is a subclass of PageId,
    // it may have its own unique fields, so need to call their equals method
    return o.equals(this);
  }

  // hashCode impl is intentionally not overridden to preserve compatibility
  // with parent class
  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
