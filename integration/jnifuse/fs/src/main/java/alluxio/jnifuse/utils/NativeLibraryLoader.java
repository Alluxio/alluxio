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
import java.util.Optional;
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
  private static final AtomicReference<LoadState> LOAD_STATE =
      new AtomicReference<>(LoadState.NOT_LOADED);

  private static final String TEMP_FILE_PREFIX = "libjnifuse";
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
      final String sharedLibraryName, final String jniLibraryName,
      final String sharedLibraryFileName, final String jniLibraryFileName,
      final String tmpDir) throws IOException {

    Optional<UnsatisfiedLinkError> err = tryLoad(() -> {
      System.loadLibrary(sharedLibraryName);
      LOG.info("Loaded {} by System.loadLibrary.", sharedLibraryName);
    });

    // No error means load is successful.
    // Just return an empty.
    if (!err.isPresent()) {
      return err;
    }

    err = tryLoad(() -> {
      System.loadLibrary(jniLibraryName);
      LOG.info("Loaded {} by System.loadLibrary.", jniLibraryName);
    });

    if (!err.isPresent()) {
      return err;
    }

    return tryLoad(() -> loadLibraryFromJar(sharedLibraryFileName, jniLibraryFileName, tmpDir));
  }

  private Optional<UnsatisfiedLinkError> load2(final String tmpDir) throws IOException {
    final String SHARED_LIBRARY_NAME =
        Environment.getSharedLibraryName("jnifuse");
    final String SHARED_LIBRARY_FILE_NAME =
        Environment.getSharedLibraryFileName("jnifuse");
    final String JNI_LIBRARY_NAME =
        Environment.getJniLibraryName("jnifuse");
    final String JNI_LIBRARY_FILE_NAME =
        Environment.getJniLibraryFileName("jnifuse");

    Optional<UnsatisfiedLinkError> err = load(
        SHARED_LIBRARY_NAME, JNI_LIBRARY_NAME, SHARED_LIBRARY_FILE_NAME, JNI_LIBRARY_FILE_NAME,
        tmpDir
    );

    if (!err.isPresent()) {
      LOG.info("Loaded libjnifuse with libfuse version 2.");
      setLoadState(LoadState.LOADED_2);
    }

    return err;
  }

  private Optional<UnsatisfiedLinkError> load3(final String tmpDir) throws IOException {
    final String SHARED_LIBRARY_NAME =
        Environment.getSharedLibraryName("jnifuse3");
    final String SHARED_LIBRARY_FILE_NAME =
        Environment.getSharedLibraryFileName("jnifuse3");
    final String JNI_LIBRARY_NAME =
        Environment.getJniLibraryName("jnifuse3");
    final String JNI_LIBRARY_FILE_NAME =
        Environment.getJniLibraryFileName("jnifuse3");

    Optional<UnsatisfiedLinkError> err = load(
        SHARED_LIBRARY_NAME, JNI_LIBRARY_NAME, SHARED_LIBRARY_FILE_NAME, JNI_LIBRARY_FILE_NAME,
        tmpDir
    );

    if (!err.isPresent()) {
      LOG.info("Loaded libjnifuse with libfuse version 3.");
      setLoadState(LoadState.LOADED_3);
    }

    return err;
  }

  /**
   * Load the library.
   *
   * @param preference     the preferred version of libfuse
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
      final VersionPreference preference, final String tmpDir) throws IOException {

    Optional<UnsatisfiedLinkError> err;

    if (preference == VersionPreference.VERSION_2) {
      err = load2(tmpDir);
      if (err.isPresent()) {
        throw err.get();
      }
      return;
    }
    if (preference == VersionPreference.VERSION_3) {
      err = load3(tmpDir);
      if (err.isPresent()) {
        throw err.get();
      }
      return;
    }

    // load libfuse2 first
    err = load2(tmpDir);
    if (!err.isPresent()) {
      return;
    }

    // if libfuse2 failed, load libfuse3
    err = load3(tmpDir);
    if (!err.isPresent()) {
      return;
    }

    // throws if neither is loaded
    throw new UnsatisfiedLinkError("Neither libfuse2 nor libfuse3 can be loaded.");
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
  void loadLibraryFromJar(final String sharedLibraryFileName,
      final String jniLibraryFileName, final String tmpDir) throws IOException {
    if (LOAD_STATE.get() == LoadState.NOT_LOADED) {
      String libPath = loadLibraryFromJarToTemp(
          sharedLibraryFileName, jniLibraryFileName, tmpDir).getAbsolutePath();
      System.load(libPath);
      LOG.info("Loaded lib by jar from path {}.", libPath);
    }
  }

  File loadLibraryFromJarToTemp(final String sharedLibraryFileName,
      final String jniLibraryFileName, final String tmpDir) throws IOException {
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
