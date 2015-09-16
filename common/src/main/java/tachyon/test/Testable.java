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

package tachyon.test;

/**
 * This interface should be implemented by classes that want to expose their private members to test
 * classes. The test classes should in turn implement the {@link Tester} interface.
 *
 * The idiomatic use of this pattern is for the tested class to encapsulate access to its private
 * members through an inner class with a private constructor:
 *
 * <code>
 * public class Foo implements Testable$lt;Foo$gt; {
 *   private int mSecret;
 *
 *   public class PrivateAccess {
 *     private PrivateAccess() {}
 *
 *     public int getSecret() { return mSecret; }
 *     public void setSecret(int secret) { mSecret = secret; }
 *   }
 *
 *   public void grantAccess(Tester$lt;Foo&gt; tester) {
 *     tester.receiveAccess(new PrivateAccess());
 *   }
 * }
 * </code>
 */
public interface Testable<T> {
  void grantAccess(Tester<T> tester);
}
