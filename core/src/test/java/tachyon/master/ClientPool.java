package tachyon.master;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Supplier;

import tachyon.client.TachyonFS;

/**
 * Keeps a collection of all clients ({@link tachyon.client.TachyonFS}) returned. The main reason
 * for this is to build cleanup clients.
 */
public final class ClientPool implements Closeable {
  private final Supplier<String> mUriSuppliers;

  private final List<TachyonFS> mClients = Collections.synchronizedList(new ArrayList<TachyonFS>());

  ClientPool(Supplier<String> uriSupplier) {
    mUriSuppliers = uriSupplier;
  }

  /**
   * Returns a {@link tachyon.client.TachyonFS} client. This client does not need to be closed
   * directly, but can be closed by calling {@link #close()} on this object.
   */
  public TachyonFS getClient() throws IOException {
    final TachyonFS fs = TachyonFS.get(mUriSuppliers.get());
    mClients.add(fs);
    return fs;
  }

  @Override
  public void close() throws IOException {
    synchronized (mClients) {
      for (TachyonFS fs : mClients) {
        fs.close();
      }

      mClients.clear();
    }
  }
}
