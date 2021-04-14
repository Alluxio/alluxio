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

package alluxio.jnifuse;

import alluxio.jnifuse.utils.NativeLibraryLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

public class LibFuse {

  private enum LibraryState {
    NOT_LOADED,
    LOADING,
    LOADED
  }

  private static AtomicReference<LibraryState> libraryLoaded =
      new AtomicReference<>(LibraryState.NOT_LOADED);

  static {
    LibFuse.loadLibrary();
  }

  public native int fuse_main_real(AbstractFuseFileSystem fs, int argc, String[] argv);

  public native ByteBuffer fuse_get_context();

  public static void loadLibrary() {
    if (libraryLoaded.get() == LibraryState.LOADED) {
      return;
    }

    if (libraryLoaded.compareAndSet(LibraryState.NOT_LOADED,
        LibraryState.LOADING)) {
      String tmpDir = System.getenv("JNIFUSE_SHAREDLIB_DIR");
      try {
        NativeLibraryLoader.getInstance().loadLibrary(tmpDir);
      } catch (IOException e) {
        libraryLoaded.set(LibraryState.NOT_LOADED);
        throw new RuntimeException("Unable to load the jni-fuse shared library"
            + e);
      }

      libraryLoaded.set(LibraryState.LOADED);
      return;
    }

    while (libraryLoaded.get() == LibraryState.LOADING) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        //ignore
      }
    }
  }
}
