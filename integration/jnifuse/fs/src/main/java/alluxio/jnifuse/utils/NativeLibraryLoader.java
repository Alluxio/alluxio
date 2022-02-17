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

package alluxio.jnifuse.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class is used to load the shared library from within the jar.
 * The shared library is extracted to a temp folder and loaded from there.
 */
public class NativeLibraryLoader {

  public enum LoadState {
    NOT_LOADED,
    LOADED_2,
    LOADED_3,
  }

  private static final Logger LOG = LoggerFactory.getLogger(NativeLibraryLoader.class);
  //singleton
  private static final NativeLibraryLoader INSTANCE = new NativeLibraryLoader();
  private static final AtomicReference<LoadState> LOAD_STATE = new AtomicReference<>(LoadState.NOT_LOADED);

  private static final String TEMP_FILE_PREFIX = "libjnifuse";
  private static final String TEMP_FILE_SUFFIX =
      Environment.getJniLibraryExtension();


  public static LoadState getLoadState() {
    return LOAD_STATE.get();
  }

  private static void setLoadState(LoadState loadState) {
    LOAD_STATE.set(loadState);
  }

  /**
   * Get a reference to the NativeLibraryLoader.
   *
   * @return The NativeLibraryLoader
   */
  public static NativeLibraryLoader getInstance() {
    return INSTANCE;
  }

  /**
   * Internal interface to write tryLoad in an elegant way
   *
   * @see Runnable interface cannot have checked exception
   */
  interface Load {
    void load() throws IOException;
  }

  /**
   * Try run the loading function.
   *
   * @param run the function to load library
   * @return true if the loading function completes successfully; false if UnsatisfiedLinkError is encountered.
   * @throws IOException if a filesystem operation fails
   */
  private boolean tryLoad(Load run) throws IOException {
    try {
      run.load();
      return true;
    } catch (final UnsatisfiedLinkError ule) {
      return false;
    }
  }

  /**
   * Load a version of libjnifuse.
   *
   * Firstly attempts to load the library from <i>java.library.path</i>,
   * if that fails then it falls back to extracting
   * the library from the classpath.
   * {@link NativeLibraryLoader#loadLibraryFromJar(String, String, String)}
   */
  private boolean load(
      final String sharedLibraryName, final String jniLibraryName,
      final String sharedLibraryFileName, final String jniLibraryFileName,
      final String tmpDir) throws IOException {
    return tryLoad(() -> {
      System.loadLibrary(sharedLibraryName);
      LOG.info("Loaded {} by System.loadLibrary.", sharedLibraryName);
    }) || tryLoad(() -> {
      System.loadLibrary(jniLibraryName);
      LOG.info("Loaded {} by System.loadLibrary.", jniLibraryName);
    }) || tryLoad(() -> {
      loadLibraryFromJar(sharedLibraryFileName, jniLibraryFileName, tmpDir);
    });
  }

  private boolean load2(final String tmpDir) throws IOException {
    final String SHARED_LIBRARY_NAME =
            Environment.getSharedLibraryName("jnifuse");
    final String SHARED_LIBRARY_FILE_NAME =
            Environment.getSharedLibraryFileName("jnifuse");
    final String JNI_LIBRARY_NAME =
            Environment.getJniLibraryName("jnifuse");
    final String JNI_LIBRARY_FILE_NAME =
            Environment.getJniLibraryFileName("jnifuse");

    if (load(SHARED_LIBRARY_NAME, JNI_LIBRARY_NAME, SHARED_LIBRARY_FILE_NAME, JNI_LIBRARY_FILE_NAME, tmpDir)) {
      LOG.info("Loaded libjnifuse with libfuse version 2.");
      setLoadState(LoadState.LOADED_2);
      return true;
    } else {
      return false;
    }
  }

  private boolean load3(final String tmpDir) throws IOException {
    final String SHARED_LIBRARY_NAME =
            Environment.getSharedLibraryName("jnifuse3");
    final String SHARED_LIBRARY_FILE_NAME =
            Environment.getSharedLibraryFileName("jnifuse3");
    final String JNI_LIBRARY_NAME =
            Environment.getJniLibraryName("jnifuse3");
    final String JNI_LIBRARY_FILE_NAME =
            Environment.getJniLibraryFileName("jnifuse3");

    if (load(SHARED_LIBRARY_NAME, JNI_LIBRARY_NAME, SHARED_LIBRARY_FILE_NAME, JNI_LIBRARY_FILE_NAME, tmpDir)) {
      LOG.info("Loaded libjnifuse with libfuse version 3.");
      setLoadState(LoadState.LOADED_3);
      return true;
    } else {
      return false;
    }
  }



  /**
   * Load the library.
   *
   * @param tmpDir A temporary directory to use
   *               to copy the native library to when loading from the classpath.
   *               If null, or the empty string, we rely on Java's
   *               {@link File#createTempFile(String, String)}
   *               function to provide a temporary location.
   *               The temporary file will be registered for deletion
   *               on exit.
   * @throws IOException if a filesystem operation fails
   */
  public synchronized void loadLibrary(final VersionPreference preference, final String tmpDir) throws IOException {

    if (preference == VersionPreference.VERSION_2) {
      load2(tmpDir);
      return;
    }
    if (preference == VersionPreference.VERSION_3) {
      load3(tmpDir);
      return;
    }

    // load libfuse2 first
    if (load2(tmpDir)) {
      return;
    }

    // if libfuse2 failed, load libfuse3
    if (load3(tmpDir)) {
      return;
    }

    // throws if neither is loaded
    // TODO better messaging
    throw new UnsatisfiedLinkError();
  }

  /**
   * Attempts to extract the native library
   * from the classpath and load it.
   *
   * @param sharedLibraryFileName The filename of the shared library file
   *                              to be loaded
   * @param jniLibraryFileName    The filename of the jni library
   *                              to be loaded
   * @param tmpDir                A temporary directory to use
   *                              to copy the native library to. If null,
   *                              or the empty string, we rely on Java's
   *                              {@link File#createTempFile(String, String)}
   *                              function to provide a temporary location.
   *                              The temporary file will be registered for deletion
   *                              on exit.
   * @throws IOException if a filesystem operation fails
   */
  void loadLibraryFromJar(final String sharedLibraryFileName, final String jniLibraryFileName, final String tmpDir)
      throws IOException {
    if (LOAD_STATE.get() == LoadState.NOT_LOADED) {
      String libPath = loadLibraryFromJarToTemp(sharedLibraryFileName, jniLibraryFileName, tmpDir).getAbsolutePath();
      System.load(libPath);
      LOG.info("Loaded lib by jar from path {}.", libPath);
    }
  }

  File loadLibraryFromJarToTemp(final String sharedLibraryFileName, final String jniLibraryFileName, final String tmpDir)
      throws IOException {
    final File temp;
    if (tmpDir == null || tmpDir.isEmpty()) {
      temp = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX);
    } else {
      temp = new File(tmpDir, jniLibraryFileName);
      if (temp.exists() && !temp.delete()) {
        throw new RuntimeException("File: " + temp.getAbsolutePath()
            + " already exists and cannot be removed.");
      }
      if (!temp.createNewFile()) {
        throw new RuntimeException("File: " + temp.getAbsolutePath()
            + " could not be created.");
      }
    }

    if (!temp.exists()) {
      throw new RuntimeException(
          "File " + temp.getAbsolutePath() + " does not exist.");
    } else {
      temp.deleteOnExit();
    }

    // attempt to copy the library from the Jar file to the temp destination
    try (final InputStream is = getClass().getClassLoader()
        .getResourceAsStream(sharedLibraryFileName)) {
      if (is == null) {
        throw new RuntimeException(
            sharedLibraryFileName + " was not found inside JAR.");
      } else {
        Files.copy(is, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);
      }
    }

    return temp;
  }

  /**
   * Private constructor to disallow instantiation.
   */
  private NativeLibraryLoader() {
  }
}
