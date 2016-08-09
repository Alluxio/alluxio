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

package alluxio.util;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.exception.ExceptionMessage;
import alluxio.util.io.PathUtils;

import io.netty.util.internal.chmv8.ConcurrentHashMapV8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Utilities to create Alluxio configurations.
 */
public final class ConfigurationUtils {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private ConfigurationUtils() {} // prevent instantiation

  /**
   * Reads a property list (key and element pairs) from the input byte stream. The input stream is
   * in a simple line-oriented format and is assumed to use the ISO 8859-1 character encoding; that
   * is each byte is one Latin1 character. Characters not in Latin1, and certain special characters,
   * are represented in keys and elements using Unicode escapes as defined in section 3.3 of
   * <cite>The Java&trade; Language Specification</cite>. The specified stream remains open after
   * this method returns.
   *
   * @param inStream the input stream
   * @param destination the destination map to store the result
   * @exception IOException if an error occurred when reading from the input stream
   * @throws IllegalArgumentException if the input stream contains a malformed Unicode escape
   *         sequence
   */
  public static void load(InputStream inStream, ConcurrentHashMapV8<String, String> destination)
      throws IOException {
    LineReader lr = new LineReader(inStream);
    char[] convtBuf = new char[1024];
    int limit;
    int keyLen;
    int valueStart;
    char c;
    boolean hasSep;
    boolean precedingBackslash;

    while ((limit = lr.readLine()) >= 0) {
      c = 0;
      keyLen = 0;
      valueStart = limit;
      hasSep = false;

      // System.out.println("line=<" + new String(mLineBuf, 0, limit) + ">");
      precedingBackslash = false;
      while (keyLen < limit) {
        c = lr.mLineBuf[keyLen];
        // need check if escaped.
        if ((c == '=' || c == ':') && !precedingBackslash) {
          valueStart = keyLen + 1;
          hasSep = true;
          break;
        } else if ((c == ' ' || c == '\t' || c == '\f') && !precedingBackslash) {
          valueStart = keyLen + 1;
          break;
        }
        if (c == '\\') {
          precedingBackslash = !precedingBackslash;
        } else {
          precedingBackslash = false;
        }
        keyLen++;
      }
      while (valueStart < limit) {
        c = lr.mLineBuf[valueStart];
        if (c != ' ' && c != '\t' && c != '\f') {
          if (!hasSep && (c == '=' || c == ':')) {
            hasSep = true;
          } else {
            break;
          }
        }
        valueStart++;
      }
      String key = loadConvert(lr.mLineBuf, 0, keyLen, convtBuf);
      String value = loadConvert(lr.mLineBuf, valueStart, limit - valueStart, convtBuf);
      destination.put(key, value);
    }
  }

  /**
   * Loads properties from resource. This method will search Classpath for the properties file with
   * the given resourceName.
   *
   * @param resourceName filename of the properties file
   * @param destination the destination map to store the result
   */
  public static void loadPropertiesFromResource(String resourceName,
      ConcurrentHashMapV8<String, String> destination) {
    InputStream inputStream =
        Configuration.class.getClassLoader().getResourceAsStream(resourceName);
    if (inputStream == null) {
      throw new RuntimeException(
          ExceptionMessage.DEFAULT_PROPERTIES_FILE_DOES_NOT_EXIST.getMessage());
    }

    try {
      load(inputStream, destination);
    } catch (IOException e) {
      LOG.error("Unable to load default Alluxio properties file {}", resourceName, e);
      throw new RuntimeException(
          ExceptionMessage.DEFAULT_PROPERTIES_FILE_DOES_NOT_EXIST.getMessage());
    }
  }

  /**
   * Loads properties from the given file. This method will search Classpath for the properties
   * file.
   *
   * @param filePath the absolute path of the file to load properties
   * @param destination the destination map to store the result
   * @return true on success, false if failed
   */
  public static boolean loadPropertiesFromFile(String filePath,
      ConcurrentHashMapV8<String, String> destination) {
    try (FileInputStream fileInputStream = new FileInputStream(filePath)) {
      load(fileInputStream, destination);
      return true;
    } catch (FileNotFoundException e) {
      return false;
    } catch (IOException e) {
      LOG.error("Unable to load properties file {}", filePath, e);
      return false;
    }
  }

  /**
   * Searches the given properties file from a list of paths as well as the classpath.
   *
   * @param propertiesFile the file to load properties
   * @param confPathList a list of paths to search the propertiesFile
   * @param destination the destination map to store the result
   */
  public static void searchPropertiesFile(String propertiesFile, String[] confPathList,
      ConcurrentHashMapV8<String, String> destination) {
    if (propertiesFile == null || confPathList == null) {
      return;
    }
    for (String path : confPathList) {
      String file = PathUtils.concatPath(path, propertiesFile);
      Boolean succeed = loadPropertiesFromFile(file, destination);
      if (succeed) {
        // If a site conf is successfully loaded, stop trying different paths
        LOG.info("Configuration file {} loaded.", file);
        break;
      }
    }
  }

  /**
   * Validates the configurations.
   *
   * @return true if the validation succeeds, false otherwise
   */
  public static boolean validateConf() {
    Set<String> validProperties = new HashSet<>();
    try {
      // Iterate over the array of Field objects in alluxio.Constants by reflection
      for (Field field : Constants.class.getDeclaredFields()) {
        if (field.getType().isAssignableFrom(String.class)) {
          // all fields are static, so ignore the argument
          String name = (String) field.get(null);
          if (name.startsWith("alluxio.")) {
            validProperties.add(name.trim());
          }
        }
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }

    // Alluxio version is a valid conf entry but not defined in alluxio.Constants
    validProperties.add(Constants.VERSION);

    // There are three properties that are auto-generated in WorkerStorage based on corresponding
    // format strings defined in alluxio.Constants. Here we transform each format string to a regexp
    // to check if a property name follows the format. E.g.,
    // "alluxio.worker.tieredstore.level%d.alias" is transformed to
    // "alluxio\.worker\.tieredstore\.level\d+\.alias".
    Pattern masterAliasPattern =
        Pattern.compile(Constants.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS_FORMAT
            .replace("%d", "\\d+").replace(".", "\\."));
    Pattern workerAliasPattern =
        Pattern.compile(Constants.WORKER_TIERED_STORE_LEVEL_ALIAS_FORMAT.replace("%d", "\\d+")
            .replace(".", "\\."));
    Pattern dirsPathPattern =
        Pattern.compile(Constants.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT
            .replace("%d", "\\d+").replace(".", "\\."));
    Pattern dirsQuotaPattern =
        Pattern.compile(Constants.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA_FORMAT.replace("%d",
            "\\d+").replace(".", "\\."));
    Pattern reservedRatioPattern =
        Pattern.compile(Constants.WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO_FORMAT.replace("%d",
            "\\d+").replace(".", "\\."));
    boolean valid = true;
    for (Map.Entry<String, String> entry : Configuration.toMap().entrySet()) {
      String propertyName = entry.getKey();
      if (masterAliasPattern.matcher(propertyName).matches()
          || workerAliasPattern.matcher(propertyName).matches()
          || dirsPathPattern.matcher(propertyName).matches()
          || dirsQuotaPattern.matcher(propertyName).matches()
          || reservedRatioPattern.matcher(propertyName).matches()) {
        continue;
      }
      if (propertyName.startsWith("alluxio.") && !validProperties.contains(propertyName)) {
        LOG.error("Unsupported or deprecated property " + propertyName);
        valid = false;
      }
    }
    return valid;
  }

  /*
   * Read in a "logical line" from an InputStream/Reader, skip all comment and blank lines and
   * filter out those leading whitespace characters (\u0020, \u0009 and \u000c) from the beginning
   * of a "natural line". Method returns the char length of the "logical line" and stores the line
   * in "mLineBuf".
   */
  static class LineReader {
    public LineReader(InputStream inStream) {
      mInStream = inStream;
      mInByteBuf = new byte[8192];
      mLineBuf = new char[1024];
    }

    byte[] mInByteBuf;
    char[] mLineBuf;
    int mInLimit = 0;
    int mInOff = 0;
    InputStream mInStream;

    int readLine() throws IOException {
      int len = 0;
      char c = 0;

      boolean skipWhiteSpace = true;
      boolean isCommentLine = false;
      boolean isNewLine = true;
      boolean appendedLineBegin = false;
      boolean precedingBackslash = false;
      boolean skipLF = false;

      while (true) {
        if (mInOff >= mInLimit) {
          mInLimit = mInStream.read(mInByteBuf);
          mInOff = 0;
          if (mInLimit <= 0) {
            if (len == 0 || isCommentLine) {
              return -1;
            }
            return len;
          }
        }
        if (mInStream != null) {
          // The line below is equivalent to calling a
          // ISO8859-1 decoder.
          c = (char) (0xff & mInByteBuf[mInOff++]);
        }
        if (skipLF) {
          skipLF = false;
          if (c == '\n') {
            continue;
          }
        }
        if (skipWhiteSpace) {
          if (c == ' ' || c == '\t' || c == '\f') {
            continue;
          }
          if (!appendedLineBegin && (c == '\r' || c == '\n')) {
            continue;
          }
          skipWhiteSpace = false;
          appendedLineBegin = false;
        }
        if (isNewLine) {
          isNewLine = false;
          if (c == '#' || c == '!') {
            isCommentLine = true;
            continue;
          }
        }

        if (c != '\n' && c != '\r') {
          mLineBuf[len++] = c;
          if (len == mLineBuf.length) {
            int newLength = mLineBuf.length * 2;
            if (newLength < 0) {
              newLength = Integer.MAX_VALUE;
            }
            char[] buf = new char[newLength];
            System.arraycopy(mLineBuf, 0, buf, 0, mLineBuf.length);
            mLineBuf = buf;
          }
          // flip the preceding backslash flag
          if (c == '\\') {
            precedingBackslash = !precedingBackslash;
          } else {
            precedingBackslash = false;
          }
        } else {
          // reached EOL
          if (isCommentLine || len == 0) {
            isCommentLine = false;
            isNewLine = true;
            skipWhiteSpace = true;
            len = 0;
            continue;
          }
          if (mInOff >= mInLimit) {
            mInLimit = mInStream.read(mInByteBuf);
            mInOff = 0;
            if (mInLimit <= 0) {
              return len;
            }
          }
          if (precedingBackslash) {
            len -= 1;
            // skip the leading whitespace characters in following line
            skipWhiteSpace = true;
            appendedLineBegin = true;
            precedingBackslash = false;
            if (c == '\r') {
              skipLF = true;
            }
          } else {
            return len;
          }
        }
      }
    }
  }

  /*
   * Converts encoded &#92;uxxxx to unicode chars and changes special saved chars to their original
   * forms
   */
  private static String loadConvert(char[] in, int off, int len, char[] convtBuf) {
    if (convtBuf.length < len) {
      int newLen = len * 2;
      if (newLen < 0) {
        newLen = Integer.MAX_VALUE;
      }
      convtBuf = new char[newLen];
    }
    char aChar;
    char[] out = convtBuf;
    int outLen = 0;
    int end = off + len;

    while (off < end) {
      aChar = in[off++];
      if (aChar == '\\') {
        aChar = in[off++];
        if (aChar == 'u') {
          // Read the xxxx
          int value = 0;
          for (int i = 0; i < 4; i++) {
            aChar = in[off++];
            switch (aChar) {
              case '0':
              case '1':
              case '2':
              case '3':
              case '4':
              case '5':
              case '6':
              case '7':
              case '8':
              case '9':
                value = (value << 4) + aChar - '0';
                break;
              case 'a':
              case 'b':
              case 'c':
              case 'd':
              case 'e':
              case 'f':
                value = (value << 4) + 10 + aChar - 'a';
                break;
              case 'A':
              case 'B':
              case 'C':
              case 'D':
              case 'E':
              case 'F':
                value = (value << 4) + 10 + aChar - 'A';
                break;
              default:
                throw new IllegalArgumentException("Malformed \\uxxxx encoding.");
            }
          }
          out[outLen++] = (char) value;
        } else {
          if (aChar == 't') {
            aChar = '\t';
          } else if (aChar == 'r') {
            aChar = '\r';
          } else if (aChar == 'n') {
            aChar = '\n';
          } else if (aChar == 'f') {
            aChar = '\f';
          }
          out[outLen++] = aChar;
        }
      } else {
        out[outLen++] = aChar;
      }
    }
    return new String(out, 0, outLen);
  }
}
