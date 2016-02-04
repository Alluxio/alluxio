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

package alluxio.exception;

import java.lang.reflect.InvocationTargetException;

import javax.annotation.concurrent.ThreadSafe;

import alluxio.thrift.AlluxioTException;

/**
 * General {@link AlluxioException} used throughout the system. It must be able serialize itself to
 * the RPC framework and convert back without losing any necessary information.
 */
@ThreadSafe
public abstract class AlluxioException extends Exception {
  private static final long serialVersionUID = 2243833925609642384L;

  private final AlluxioExceptionType mType;

  /**
   * Constructs a {@link AlluxioException} with an exception type from a {@link AlluxioTException}.
   *
   * @param te the type of the exception
   */
  public AlluxioException(AlluxioTException te) {
    super(te.getMessage());
    mType = AlluxioExceptionType.valueOf(te.getType());
  }

  protected AlluxioException(AlluxioExceptionType type, Throwable cause) {
    super(cause);
    mType = type;
  }

  protected AlluxioException(AlluxioExceptionType type, String message) {
    super(message);
    mType = type;
  }

  protected AlluxioException(AlluxioExceptionType type, String message, Throwable cause) {
    super(message, cause);
    mType = type;
  }

  /**
   * Constructs a {@link AlluxioTException} from a {@link AlluxioException}.
   *
   * @return a {@link AlluxioTException} of the type of this exception
   */
  public AlluxioTException toAlluxioTException() {
    return new AlluxioTException(mType.name(), getMessage());
  }

  /**
   * Constructs a {@link AlluxioException} from a {@link AlluxioTException}.
   *
   * @param e the {link AlluxioTException} to convert to a {@link AlluxioException}
   * @return a {@link AlluxioException} of the type specified in e, with the message specified in e
   */
  public static AlluxioException from(AlluxioTException e) {
    AlluxioExceptionType exceptionType = AlluxioExceptionType.valueOf(e.getType());
    Class<? extends AlluxioException> throwClass = exceptionType.getExceptionClass();
    Exception reflectError;
    try {
      AlluxioException throwInstance =
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
}
