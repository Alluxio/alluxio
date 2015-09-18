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

package tachyon.util.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;

/**
 * Utility methods about serialization/deserialization.
 */
public final class SerDeUtils {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  public static byte[] objectToByteArray(Object obj) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutput out = null;
    try {
      out = new ObjectOutputStream(baos);
      out.writeObject(obj);
      return baos.toByteArray();
    } catch (IOException e) {
      LOG.warn("Failed to write into the byte array", e);
    } finally {
      try {
        if (out != null) {
          out.close();
        }
      } catch (IOException e) {
        LOG.warn("Failed to close the ObjectOutput stream", e);
      }
      try {
        baos.close();
      } catch (IOException e) {
        LOG.warn("Failed to close the ByteArrayOutputStream", e);
      }
    }
    return null;
  }

  public static Object byteArrayToObject(byte[] bytes) {
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    ObjectInput in = null;
    try {
      in = new ObjectInputStream(bais);
      return in.readObject();
    } catch (IOException e) {
      LOG.warn("Failed to read from the byte array", e);
    } catch (ClassNotFoundException e) {
      LOG.warn("The class cannot be found", e);
    } finally {
      try {
        bais.close();
      } catch (IOException e) {
        LOG.warn("Failed to close the ByteArrayInputStream", e);
      }
      try {
        if (in != null) {
          in.close();
        }
      } catch (IOException e) {
        LOG.warn("Failed to close the ObjectInput stream", e);
      }
    }
    return null;
  }

  private SerDeUtils() {} // prevent instantiation
}
