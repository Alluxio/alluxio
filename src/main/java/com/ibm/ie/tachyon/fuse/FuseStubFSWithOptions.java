package com.ibm.ie.tachyon.fuse;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jnr.ffi.Struct;
import ru.serce.jnrfuse.FuseException;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.utils.SecurityUtils;

public class FuseStubFSWithOptions extends FuseStubFS {
  private final static Logger LOG = LoggerFactory.getLogger(FuseStubFSWithOptions.class);

  private static final int TIMEOUT = 2000;

  /**
   * Slightly modifies {@link ru.serce.jnrfuse.AbstractFuseFS#mount} by allowing to pass in
   * a set of {@code fuseOpts} that will be passed to the FUSE mount command.
   */
  public void mount(Path mountPoint, boolean blocking, boolean debug, String[] fuseOpts) {
    if (!mounted.compareAndSet(false, true)) {
      throw new FuseException("Fuse fs already mounted!");
    }
    this.mountPoint = mountPoint;
    String[] arg;
    if (!debug) {
      arg = new String[]{getFSName(), "-f", mountPoint.toAbsolutePath().toString()};
    } else {
      arg = new String[]{getFSName(), "-f", "-d", mountPoint.toAbsolutePath().toString()};
    }
    final int oldLength = arg.length;
    arg = Arrays.copyOf(arg, oldLength + fuseOpts.length);
    System.arraycopy(fuseOpts,0, arg, oldLength, fuseOpts.length);

    final String[] args = arg;
    LOG.debug("FUSE mount options: " + Arrays.toString(args));



    try {
      if (!Files.isDirectory(mountPoint)) {
        throw new FuseException("Mount point should be directory");
      }
      if (SecurityUtils.canHandleShutdownHooks()) {
        java.lang.Runtime.getRuntime().addShutdownHook(new Thread(this::umount));
      }
      int res;
      if (blocking) {
        res = execMount(arg);
      } else {
        try {
          res = CompletableFuture
              .supplyAsync(() -> execMount(args))
              .get(TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
          // ok
          res = 0;
        }
      }
      if (res != 0) {
        throw new FuseException("Unable to mount FS, return code = " + res);
      }
    } catch (Exception e) {
      mounted.set(false);
      throw new FuseException("Unable to mount FS", e);
    }
  }

  private int execMount(String[] arg) {
    return libFuse.fuse_main_real(arg.length, arg, fuseOperations, Struct.size(fuseOperations), null);
  }


}
