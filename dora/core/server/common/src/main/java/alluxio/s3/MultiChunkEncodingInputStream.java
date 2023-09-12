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

package alluxio.s3;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This input stream decodes a set of {@code aws-chunked} encoded input content buffer into its
 * original form.
 * For more informaiton about the aws-chunked encoding type, see the AWS S3 REST API reference
 * https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html
 */
@NotThreadSafe
public class MultiChunkEncodingInputStream extends InputStream {
  /** the current Content. */
  private ByteBuf mCurrentContent;
  /** the current chunk's total length. */
  private int mCurrentChunkLength = -1;
  /** the index which we've read up to in the current chunk. */
  private int mCurrentChunkIdx = 0;
  private ByteBufInputStream mStream;
  private String mHoldMessage;
  private int mSkipIdx = 0;

  private static final int CHUNK_HEADER_LENGTHEN = 82;

  /**
   * Constructs an {@link MultiChunkEncodingInputStream} with first currentContent.
   * @param currentContent
   */
  public MultiChunkEncodingInputStream(ByteBuf currentContent) {
    mCurrentContent = currentContent;
    mStream = new ByteBufInputStream(currentContent);
  }

  /**
   * Sets currentContent with given content.
   * @param currentContent
   */
  public void setCurrentContent(ByteBuf currentContent) {
    mCurrentContent = currentContent;
    mStream = new ByteBufInputStream(currentContent);
  }

  /**
   * Decodes the chunk headers and advances the underlying input stream up to the next data.
   * Returns immediately if the current chunk hasn't been fully read yet.
   *
   * @throws IOException an error is encountered interacting with the underlying IO stream
   */
  private void decodeChunkHeader() throws IOException {
    if (mCurrentChunkIdx < mCurrentChunkLength) {
      return;
    }
    // The chunk header format
    // hexLen + ";chunk-signature".length() + signature.length + "\r\n".length();
    // see https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html for
    // further information. Everything after the "hexLen" is unchanging
    // read bytes from input stream until hitting a semicolon
    StringBuilder hexLen = new StringBuilder();
    if (!StringUtils.isEmpty(mHoldMessage)) {
      hexLen.append(mHoldMessage);
    }
    int semi = ';';
    int read;
    // slow, but is there a better way without reading over the chunk header and having to rewind
    // on the stream? The length is generally only a few characters, so the performance impact
    // should be minimal using read(). I've opted for code readability over performance in this
    // particular case
    while ((read = mStream.read()) != semi) {
      switch (read) {
        case -1:
          mHoldMessage = hexLen.toString();
          return;
        case (int) '\r':
        case (int) '\n':
          continue;
        default:
          hexLen.append((char) read);
      }
    }
    mCurrentChunkLength = Integer.parseInt(hexLen.toString(), 16);
    mCurrentChunkIdx = 0;
    mHoldMessage = null;

    // This is the constant size of the chunk header *after* hexLen described in the comments
    // above.
    int totalSkipped = 0;
    do {
      totalSkipped += mStream.skip(CHUNK_HEADER_LENGTHEN - totalSkipped);
      if (mCurrentContent.readableBytes() <= 0) {
        mSkipIdx = totalSkipped;
        return;
      }
    } while (totalSkipped < CHUNK_HEADER_LENGTHEN);
    mSkipIdx = 0;
  }

  @Override
  public int read() throws IOException {
    decodeChunkHeader();
    int ret = mStream.read();
    if (ret != -1) {
      mCurrentChunkIdx += 1;
    }
    return ret;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    /*
    Sample chunk:
    400;chunk-signature=0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497
    <1024 bytes>
    0;chunk-signature=b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9

    ------
     The general idea is for this method to just read up to the next chunk header.
     If we're already at the end of a chunk in the stream, decode the next one, and then write
     as much as possible from the next chunk in the user's buffer.
     */
    if (mSkipIdx != 0) {
      int totalSkipped = mSkipIdx;
      do {
        totalSkipped += mStream.skip(CHUNK_HEADER_LENGTHEN - totalSkipped);
        if (mCurrentContent.readableBytes() <= 0) {
          mSkipIdx = totalSkipped;
          return -1;
        }
      } while (totalSkipped < CHUNK_HEADER_LENGTHEN);
      mSkipIdx = 0;
    }

    // only copy up to the end of the buffer
    int bytesToRead = Math.min((b.length - off), len);
    // if we go over the chunk in the desired request, then we need to decode first
    if (bytesToRead + mCurrentChunkIdx >= mCurrentChunkLength) {
      // read the next portion of input up to the end of the current chunk, decoding this chunk
      // if necessary
      decodeChunkHeader();
      // take the smaller of the two to ensure we don't go past a chunk boundary
      int toRead = Math.min(mCurrentChunkLength - mCurrentChunkIdx, bytesToRead);
      if (toRead <= 0) {
        return -1;
      }
      // do the read and advanced the chunk pointer
      int totalRead = mStream.read(b, off, toRead);
      if (totalRead == -1) {
        return totalRead;
      }
      mCurrentChunkIdx += totalRead;
      return totalRead;
    } else {
      int totalRead = mStream.read(b, off, bytesToRead);
      if (totalRead != -1) {
        mCurrentChunkIdx += totalRead;
      }
      return totalRead;
    }
  }
}
