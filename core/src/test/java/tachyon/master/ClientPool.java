package tachyon.master;

import com.google.common.base.Supplier;
import org.apache.thrift.TException;
import tachyon.client.TachyonFS;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class ClientPool implements Closeable {
  private final Supplier<String> uriSupplier;

  private final List<TachyonFS> mClients =
      Collections.synchronizedList(new ArrayList<TachyonFS>());

  ClientPool(Supplier<String> uriSupplier) {
    this.uriSupplier = uriSupplier;
  }

  public TachyonFS getClient() throws IOException {
    final TachyonFS fs = TachyonFS.get(uriSupplier.get());
    mClients.add(fs);
    return fs;
  }

  @Override
  public void close() throws IOException {
    synchronized (mClients) {
      for (TachyonFS fs : mClients) {
        try {
          fs.close();
        } catch (TException e) {
          throw new IOException(e);
        }
      }

      mClients.clear();
    }
  }
}
