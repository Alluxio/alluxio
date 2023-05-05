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

package alluxio.client.file.dora.netty;

import com.github.oxo42.stateless4j.delegates.Trace;
import org.slf4j.Logger;

/**
 * A tracer that logs every trigger and state transition to the debug level.
 *
 * @param <S> state type
 * @param <T> trigger type
 */
public class DebugLoggingTracer<S, T> implements Trace<S, T> {
  private final Logger mLogger;

  /**
   * Constructor.
   *
   * @param logger logger of the state machine
   */
  public DebugLoggingTracer(Logger logger) {
    mLogger = logger;
  }

  @Override
  public void trigger(T trigger) {
    mLogger.debug("Trigger fired: {}", trigger);
  }

  @Override
  public void transition(T trigger, S source, S destination) {
    mLogger.debug("State transitioned from {} to {} because of trigger {}",
        source, destination, trigger);
  }
}
