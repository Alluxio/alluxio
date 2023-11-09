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

package alluxio.cachestore.utils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to load the shared library from within the jar.
 * The shared library is extracted to a temp folder and loaded from there.
 */
public class NativeLibraryLoader {

  public enum LoadState {
    NOT_LOADED,
    LOADED_1,
  }

  private static final Logger LOG = LoggerFactory.getLogger(NativeLibraryLoader.class);
  //singleton
  private static final NativeLibraryLoader INSTANCE = new NativeLibraryLoader();
  private static final AtomicReference<LoadState> LOAD_STATE =
      new AtomicReference<>(LoadState.NOT_LOADED);

  private static final String TEMP_FILE_PREFIX = "libjnicachestore";
  private static final String TEMP_FILE_SUFFIX = Environment.getJniLibraryExtension();

  /**
   * @return the load state
   */
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
   * Internal interface to write tryLoad in an elegant way.
   *
   * @see Runnable interface cannot have checked exception
   */
  interface Load {
    void load() throws IOException;
  }

  /**
   * Try running the loading function.
   *
   * @param load the function to load library
   * @return the error if UnsatisfiedLinkError is encountered, empty if it completes successfully
   * @throws IOException if a filesystem operation fails
   */
  private Optional<UnsatisfiedLinkError> tryLoad(Load load) throws IOException {
    try {
      load.load();
      return Optional.empty();
    } catch (final UnsatisfiedLinkError ule) {
      return Optional.of(ule);
    }
  }

  /**
   * Load a version of libjnifuse.
   * <p>
   * Firstly attempts to load the library from <i>java.library.path</i>,
   * if that fails then it falls back to extracting
   * the library from the classpath.
   * {@link NativeLibraryLoader#loadLibraryFromJar(String, String, String)}
   *
   * @return the error why load is failed. Empty if load is successful
   */
  private Optional<UnsatisfiedLinkError> load(
      final String tmpDir) throws IOException {
    final String sharedLibraryName = TEMP_FILE_PREFIX + TEMP_FILE_SUFFIX;
    final String shareJarName = System.getenv("CACHESTORE_SHARED_JAR_PATH");

    Optional<UnsatisfiedLinkError> err = tryLoad(() -> {
      System.load(tmpDir + "/" + sharedLibraryName);
      LOG.info("Loaded {} by System.loadLibrary.", sharedLibraryName);
    });

    if (err.isPresent()) {
      System.out.println(err.toString());
      err = tryLoad(() -> {
        System.loadLibrary(sharedLibraryName);
        LOG.info("Loaded {} by System.loadLibrary.", sharedLibraryName);
      });
    }

    if (err.isPresent()) {
      System.out.println(err.toString());
      err = tryLoad(() -> loadLibraryFromJar(shareJarName, sharedLibraryName, tmpDir));
    }

    if (!err.isPresent()) {
      setLoadState(LoadState.LOADED_1);
    }
    return err;
  }


  /**
   * Load the library.
   *
   * @param version the version of libfuse to load
   * @param tmpDir A temporary directory to use
   *               to copy the native library to when loading from the classpath.
   *               If null, or the empty string, we rely on Java's
   *               {@link File#createTempFile(String, String)}
   *               function to provide a temporary location.
   *               The temporary file will be registered for deletion
   *               on exit.
   * @throws IOException if a filesystem operation fails
   */
  public synchronized void loadLibrary(
      final LibCacheStoreVersion version, final String tmpDir) throws IOException {

    Optional<UnsatisfiedLinkError> err;
    switch (version) {
      case VERSION_1:
        err = load(tmpDir);
        break;
      default:
        // should not fall here
        throw new RuntimeException(String.format("Unsupported libjnicachestore version %d",
            version));
    }
    if (err.isPresent()) {
      throw err.get();
    }
    return;
  }

  /**
   * Attempts to extract the native library
   * from the classpath and load it.
   *
   * @param sharedLibraryFileName The filename of the shared library file
   *                              to be loaded
   * @param cacheStoreLibraryFileName    The filename of the jni library
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
  void loadLibraryFromJar(final String sharedLibraryFileName,
      final String cacheStoreLibraryFileName, final String tmpDir) throws IOException {
    if (LOAD_STATE.get() == LoadState.NOT_LOADED) {
      String libPath = loadLibraryFromJarToTemp(
          sharedLibraryFileName, cacheStoreLibraryFileName, tmpDir).getAbsolutePath();
      System.load(libPath);
      LOG.info("Loaded lib by jar from path {}.", libPath);
    }
  }

  File loadLibraryFromJarToTemp(final String sharedLibraryFileName,
      final String cacheStoreLibraryFileName, final String tmpDir) throws IOException {
    final File temp;
    if (tmpDir == null || tmpDir.isEmpty()) {
      temp = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX);
    } else {
      temp = new File(tmpDir, cacheStoreLibraryFileName);
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

  /**
   * The libfuse version set by the user.
   */
  public enum LibCacheStoreVersion {
    VERSION_1,
  }
}
