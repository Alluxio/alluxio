/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.command.permission;

import java.util.regex.Pattern;

import tachyon.thrift.ClientFileInfo;

/**
 * Parse a permission mode passed in from a chmod command and apply that mode against an existing
 * file.
 */
public class ChmodParser extends PermissionParser {
  private static Pattern sChmodOctalPattern = Pattern.compile("^\\s*[+]?([01]?)([0-7]{3})\\s*$");
  private static Pattern sChmodNormalPattern = Pattern
      .compile("\\G\\s*([ugoa]*)([+=-]+)([rwxXt]+)([,\\s]*)\\s*");

  public ChmodParser(String modeStr) throws IllegalArgumentException {
    super(modeStr, sChmodNormalPattern, sChmodOctalPattern);
  }

  /**
   * Apply permission against specified file and determine what the new mode would be
   * 
   * @param status File against which to apply mode
   * @return File's new mode if applied.
   */
  public short applyNewPermission(ClientFileInfo status) {
    int existing = status.getPermission();
    boolean exeOk = status.isFolder || (existing & 0111) != 0;

    return (short) combineModes(existing, exeOk);
  }
}
