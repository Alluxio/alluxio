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

package alluxio.proxy.s3;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This input stream decodes an {@code aws-chunked} encoded input stream into its original form.
 * For more informaiton about the aws-chunked encoding type, see the AWS S3 REST API reference
 * https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html
 */
@NotThreadSafe
public class ChunkedEncodingInputStream extends FilterInputStream {

  /** the current chunk's total length. */
  private int mCurrentChunkLength = -1;
  /** the index which we've read up to in the current chunk. */
  private int mCurrentChunkIdx = 0;

  /**
   * Creates a <code>FilterInputStream</code>
   * by assigning the  argument <code>in</code>
   * to the field <code>this.in</code> so as
   * to remember it for later use.
   *
   * @param in the underlying input stream, or <code>null</code> if
   *           this instance is to be created without an underlying stream.
   */
  protected ChunkedEncodingInputStream(InputStream in) {
    super(in);
  }

  @Override
  public int read() throws IOException {
    decodeChunkHeader();
    int ret = super.read();
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
      int totalRead = in.read(b, off, toRead);
      if (totalRead == -1) {
        return totalRead;
      }
      mCurrentChunkIdx += totalRead;
      return totalRead;
    } else {
      int totalRead = in.read(b, off, bytesToRead);
      if (totalRead != -1) {
        mCurrentChunkIdx += totalRead;
      }
      return totalRead;
    }
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
    int semi = ';';
    int read;
    // slow, but is there a better way without reading over the chunk header and having to rewind
    // on the stream? The length is generally only a few characters, so the performance impact
    // should be minimal using read(). I've opted for code readability over performance in this
    // particular case
    while ((read = in.read()) != semi) {
      switch (read) {
        case -1:
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

    // This is the constant size of the chunk header *after* hexLen described in the comments
    // above.
    int chunkHeaderLen = 82;
    // TODO(zac): verify the chunk header
    long totalSkipped = 0;
    do {
      totalSkipped += in.skip(chunkHeaderLen - totalSkipped);
    } while (totalSkipped < chunkHeaderLen);
  }
}
