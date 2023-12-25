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

package alluxio.fuse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;
import sun.misc.SignalHandler;

/**
 * Respond to the kill command when in mount fuse JVM.
 * If it does not respond, the subsequent shutdown hook will not be triggered.
 *
 * Fuse will have its own signal handler installed as part of fuse_main_real
 * with fuse_set_signal_handlers for SIGINT/SIGTERM, but it will not replace
 * any existing signal handlers installed up front. Since jvm always install
 * signal handlers therefore libfuse is never able to do so.
 * But disabling JVM's signal handlers by flag -Xrs, fuse acts differently
 * on receiving these signals on different platform(MacOS can receive/Linux can't)
 * Therefore we always let 'umount' or 'fusermount' to instruct libfuse
 * to stop serving, only when it won't respond, we rely on this SignalHandler to
 * act on these signals to shutdown ourselves.
 */
public class FuseSignalHandler implements SignalHandler {
  private static final Logger LOG = LoggerFactory.getLogger(FuseSignalHandler.class);

  /**
   * Use to umount Fuse application during stop.
   */
  private final AlluxioJniFuseFileSystem mFuseUmountable;

  /**
   * Constructs the new {@link FuseSignalHandler}.
   * @param fuseUmountable mounted fuse application
   */
  public FuseSignalHandler(AlluxioJniFuseFileSystem fuseUmountable) {
    mFuseUmountable = fuseUmountable;
  }

  @Override
  public void handle(Signal signal) {
    LOG.info("Receive signal name {}, number {}, system exiting",
        signal.getName(), signal.getNumber());
    int number = signal.getNumber();
    // SIGTERM - 15
    // SIGINT - 2
    if (number == 15 || number == 2) {
      try {
        mFuseUmountable.destroy();
      } catch (Throwable t) {
        LOG.error("Unable to umount fuse. exiting anyways...", t);
      }
    }
    System.exit(0);
  }
}
