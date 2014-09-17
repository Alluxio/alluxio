package tachyon.client;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

/**
 * All interactions for writing to a block are done through this common interface. Any restrictions
 * that are imposed on a block (such as length) will be done in the
 * {@link #write(java.nio.ByteBuffer)} method, which will return {@code -1} for any violations.
 * 
 * To access a instance of this class, callers are expected to go through
 * {@link tachyon.client.Blocks#createWritableBlock(TachyonFS, long, long)}.
 * 
 * Implementations are not expected to be safe under concurrency, so the caller is expected to
 * handle any concurrency needs required.
 * 
 * @see tachyon.client.BufferedWritableBlockChannel
 * @see tachyon.client.BoundedWritableBlockChannel
 * @see tachyon.client.LocalWritableBlockChannel
 */
interface WritableBlockChannel extends WritableByteChannel {

  /**
   * Similar to {@link #close()}, but indicates that any permanent resources should be freed.
   * 
   * @throws IOException
   */
  void cancel() throws IOException;
}
