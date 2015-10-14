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

package tachyon.exception;

import java.lang.reflect.InvocationTargetException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.thrift.TachyonTException;

/**
 * General TachyonException used throughout the system. It must be able serialize itself to the RPC
 * framework and convert back without losing any necessary information.
 */
public class TachyonException extends Exception {
  private TachyonExceptionType mType;
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  public TachyonException(TachyonTException te) {
    super(te.getMessage());
    mType = TachyonExceptionType.valueOf(te.type);
  }

  protected TachyonException(TachyonExceptionType type, Throwable cause) {
    super(cause);
    mType = type;
  }

  protected TachyonException(TachyonExceptionType type, String message) {
    super(message);
    mType = type;
  }

  protected TachyonException(TachyonExceptionType type, String message, Throwable cause) {
    super(message, cause);
    mType = type;
  }

  public TachyonExceptionType getType() {
    return mType;
  }

  public TachyonTException toTachyonTException() {
    return new TachyonTException(mType.name(), getMessage());
  }

  public static <T extends TachyonException> void unwrap(TachyonException e, Class<T> throwClass)
      throws T {
    try {
      T throwInstance =
          throwClass.getConstructor(String.class, Throwable.class).newInstance(e.getMessage(), e);
      if (e.getType() == throwInstance.getType()) {
        throw throwInstance;
      }
      // This will be easier when we move to Java 7, when instead of catching Exception we can catch
      // ReflectiveOperationException, the Java 7 superclass for these Exceptions
    } catch (InstantiationException e1) {
      handleUnwrapExceptionError(throwClass);
    } catch (IllegalAccessException e1) {
      handleUnwrapExceptionError(throwClass);
    } catch (InvocationTargetException e1) {
      handleUnwrapExceptionError(throwClass);
    } catch (NoSuchMethodException e1) {
      handleUnwrapExceptionError(throwClass);
    }
  }

  private static void handleUnwrapExceptionError(Class<?> throwClass) {
    // They passed us an exception class that couldn't be instantiated with a string and
    // throwable, so we can ignore it
    LOG.error("Class passed to unwrap is invalid: ", throwClass.getName());
  }
}
