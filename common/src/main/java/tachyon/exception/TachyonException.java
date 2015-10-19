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

import tachyon.thrift.TachyonTException;

/**
 * General TachyonException used throughout the system. It must be able serialize itself to the RPC
 * framework and convert back without losing any necessary information.
 */
public abstract class TachyonException extends Exception {
  private TachyonExceptionType mType;

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

  /**
   * Construct a TachyonException from a TachyonTException.
   *
   * @param e The TachyonTException to convert to a TachyonException
   * @return A TachyonException of the type specified in e, with the message specified in e
   */
  public static TachyonException of(TachyonTException e) {
    TachyonExceptionType exceptionType = TachyonExceptionType.valueOf(e.type);
    Class<? extends TachyonException> throwClass = exceptionType.getExceptionClass();
    Exception reflectError;
    try {
      TachyonException throwInstance =
          throwClass.getConstructor(String.class).newInstance(e.getMessage());
      return throwInstance;
      // This will be easier when we move to Java 7, when instead of catching Exception we can catch
      // ReflectiveOperationException, the Java 7 superclass for these Exceptions
    } catch (InstantiationException e1) {
      reflectError = e1;
    } catch (IllegalAccessException e1) {
      reflectError = e1;
    } catch (InvocationTargetException e1) {
      reflectError = e1;
    } catch (NoSuchMethodException e1) {
      reflectError = e1;
    }
    String errorMessage = "Could not instantiate " + throwClass.getName() + " with a String-only "
        + "constructor: " + reflectError.getMessage();
    throw new IllegalStateException(errorMessage);
  }

  /**
   * If the given TachyonException is of the given class, throw it.
   *
   * @param e The TachyonException
   * @param throwClass The type of exception to throw e is of the right type
   * @throws T If e is of type T
   */
  public static <T extends TachyonException> void unwrap(TachyonException e, Class<T> throwClass)
      throws T {
    if (throwClass.isInstance(e)) {
      throw throwClass.cast(e);
    }
  }
}
