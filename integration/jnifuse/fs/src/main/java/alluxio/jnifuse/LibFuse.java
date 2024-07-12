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

import alluxio.jnifuse.utils.LibfuseVersion;
import alluxio.jnifuse.utils.NativeLibraryLoader;

import java.io.IOException;
import java.nio.ByteBuffer;

public class LibFuse {

  public native int fuse_main_real(AbstractFuseFileSystem fs, int argc, String[] argv);

  public native ByteBuffer fuse_get_context();

  private static LibfuseVersion LIB_FUSE_VERSION;

  public static void loadLibrary(LibfuseVersion version) {
    String tmpDir = System.getenv("JNIFUSE_SHAREDLIB_DIR");
    String libName = getJniLibName(version);
    try {
      NativeLibraryLoader.getInstance().loadLibrary(libName, tmpDir);
      LIB_FUSE_VERSION = version;
    } catch (IOException e) {
      throw new RuntimeException("Unable to load the jni-fuse shared library"
          + e);
    }

    while (NativeLibraryLoader.getLoadState() == NativeLibraryLoader.LoadState.LOADING) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        //ignore
      }
    }
  }

  private static String getJniLibName(
      final LibfuseVersion version) {
    switch (version) {
      case VERSION_2:
        return "jnifuse";
      case VERSION_3:
        return "jnifuse3";
      default:
        // should not fall here
        throw new RuntimeException(String.format("Unsupported libfuse version %d", version));
    }
  }

  public static LibfuseVersion getLibFuseVersion() {
    return LIB_FUSE_VERSION;
  }
}
