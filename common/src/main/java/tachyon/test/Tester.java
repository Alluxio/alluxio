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
 * This interface should be implemented by test classes that want to gain access to private members
 * of classes they test. The tested classes should in turn implement the {@link Testable} interface.
 *
 * The idiomatic use of this pattern is for the test class to capture the access object in a private
 * member:
 *
 * <code>
 * public class FooTest implements Tester$lt;Foo$gt; {
 *   Foo.PrivateAccess mPrivateAccess;
 *
 *   public void receiveAccess(Object access) { mPrivateAccess = (Foo.PrivateAccess) access; }
 *
 *   &#64;Test
 *   public void SecretTest() {
 *     Foo foo = new Foo();
 *     foo.grantAccess(this);
 *     mPrivateAccess.setSecret(10);
 *     Assert.equals(mPrivateAccess.getSecret(), 10);
 *   }
 * }
 * </code>
 */
public interface Tester<T> {
  void receiveAccess(Object access);
}
