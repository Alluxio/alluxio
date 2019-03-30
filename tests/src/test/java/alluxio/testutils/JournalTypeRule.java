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

package alluxio.testutils;

import alluxio.master.journal.JournalType;

import org.junit.AssumptionViolatedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * A rule that is used to enforce journal type before running this test.
 * Value for system property, "alluxio.test.journal.type", is injected by surefire plugin.
 */
public final class JournalTypeRule implements TestRule {
  JournalType mJournalType;

  @Override
  public Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        String journalType = System.getProperty("alluxio.test.journal.type");
        if (journalType == null || journalType.equals(JournalType.EMBEDDED.toString())) {
          mJournalType = JournalType.EMBEDDED;
        } else if (journalType.equals(JournalType.UFS.toString())) {
          mJournalType = JournalType.UFS;
        } else {
          throw new AssumptionViolatedException("Journal type not supported. Skipping test!");
        }
        base.evaluate();
      }
    };
  }

  public JournalType getJournalType() {
    return mJournalType;
  }
}
