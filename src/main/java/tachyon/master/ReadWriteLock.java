/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.master;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A simple read-write lock. Right now writes can be starved, but that
 * shouldn't be a big deal for this kind of workload.
*/
public class ReadWriteLock {
    private AtomicInteger readers = new AtomicInteger(0);
    private AtomicBoolean writer = new AtomicBoolean(false);

    public void readLock() {
        while (writer.get()) {}
        readers.getAndIncrement();
        if (writer.get()) {
            readers.getAndDecrement();
            readLock();
        }
    }

    public void readUnlock() {
        readers.getAndDecrement();
    }

    public void writeLock() {
        while (readers.get() > 0 || !writer.compareAndSet(false, true)) {}
        if (readers.get() > 0) {
            writer.set(false);
            writeLock();
        }
    }

    public void writeUnlock() {
        writer.set(false);
    }

    public void upgrade() {
        while (readers.get() > 1 || !writer.compareAndSet(false, true)) {}
        if (readers.get() > 1) {
            writer.set(false);
            upgrade();
        }
        readUnlock();
    }
}
