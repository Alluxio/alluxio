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

package alluxio.master.diagnostic;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.master.MasterContext;
import alluxio.master.MasterFactory;
import alluxio.master.MasterRegistry;
import alluxio.master.SafeModeManager;
import alluxio.master.journal.JournalSystem;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory to create a {@link DiagnosticMaster} instance.
 */
@ThreadSafe
public final class DiagnosticMasterFactory implements MasterFactory {
  private static final Logger LOG = LoggerFactory.getLogger(DiagnosticMasterFactory.class);

  /**
   * Constructs a new {@link DiagnosticMasterFactory}.
   */
  public DiagnosticMasterFactory() {}

  @Override
  public boolean isEnabled() {
    return Configuration.getEnum(PropertyKey.DIAGNOSTIC_LOG_LEVEL,
        DiagnosticLogLevel.class) != DiagnosticLogLevel.NONE;
  }

  @Override
  public String getName() {
    return Constants.DIAGNOSTIC_MASTER_NAME;
  }

  @Override
  public DiagnosticMaster create(MasterRegistry registry, JournalSystem journalSystem,
      SafeModeManager safeModeManager) {
    if (!isEnabled()) {
      return null;
    }
    Preconditions.checkArgument(journalSystem != null, "journal system may not be null");
    LOG.info("Creating {} with diagnostic log level {}", DiagnosticMaster.class.getName(),
        Configuration.getEnum(PropertyKey.DIAGNOSTIC_LOG_LEVEL, DiagnosticLogLevel.class));
    return new DiagnosticMaster(registry, new MasterContext(journalSystem, safeModeManager));
  }
}
