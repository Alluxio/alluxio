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

package alluxio.master.backcompat;

import java.io.File;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class representing a journal used by the backwards compatibility test.
 */
public final class Journal {
  private static final String VERSION = "(?<major>\\d+).(?<minor>\\d+).(?<patch>\\d+).*";
  private static final Pattern JOURNAL_VERSION_RE = Pattern.compile("journal-" + VERSION);
  private static final Pattern BACKUP_VERSION_RE = Pattern.compile("backup-" + VERSION);

  private final boolean mIsBackup;
  private final String mDir;
  private final Version mVersion;

  private Journal(boolean isBackup, String dir, Version version) {
    mIsBackup = isBackup;
    mDir = dir;
    mVersion = version;
  }

  /**
   * @return whether this is a journal backup, as opposed to a full journal directory
   */
  public boolean isBackup() {
    return mIsBackup;
  }

  /**
   * @return the absolute directory of the journal
   */
  public String getDir() {
    return mDir;
  }

  /**
   * @return the version of the journal
   */
  public Version getVersion() {
    return mVersion;
  }

  /**
   * @param path an absolute path of an Alluxio journal or journal backup
   * @return the constructed Journal
   */
  public static Optional<Journal> parse(String path) {
    File f = new File(path);
    boolean isBackup = false;
    Matcher matcher = JOURNAL_VERSION_RE.matcher(f.getName());
    if (!matcher.matches()) {
      isBackup = true;
      matcher = BACKUP_VERSION_RE.matcher(f.getName());
      if (!matcher.matches()) {
        return Optional.empty();
      }
    }
    int major = Integer.parseInt(matcher.group("major"));
    int minor = Integer.parseInt(matcher.group("minor"));
    int patch = Integer.parseInt(matcher.group("patch"));
    return Optional.of(new Journal(isBackup, path, new Version(major, minor, patch)));
  }
}
