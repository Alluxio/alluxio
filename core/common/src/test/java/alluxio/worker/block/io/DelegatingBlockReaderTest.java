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

package alluxio.worker.block.io;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.Closeable;
import java.io.IOException;

/**
 * Tests for the {@link DelegatingBlockReader} class.
 */
public class DelegatingBlockReaderTest {

    private DelegatingBlockReader mReader;

    /** Rule to create a new temporary folder during each test. */
    @Rule
    public TemporaryFolder mFolder = new TemporaryFolder();

    /**
     * Test the {@link DelegatingBlockReader#toString()} method when the underlying BlockReader is null.
     */
    @Test
    public void toStringWithNullBlockReader() throws Exception {
        mReader = new DelegatingBlockReader(null, new Closeable() {
            @Override
            public void close() throws IOException {
                // Empty close implementation for testing
            }
        });

        String result = mReader.toString();
        Assert.assertEquals("DelegatingBlockReader not fully initialized.", result);
    }

    /**
     * Test the {@link DelegatingBlockReader#toString()} method when the underlying BlockReader is valid.
     */
    @Test
    public void toStringWithValidBlockReader() throws Exception {
        byte[] data = "test data".getBytes();
        MockBlockReader mockBlockReader = new MockBlockReader(data);

        mReader = new DelegatingBlockReader(mockBlockReader, new Closeable() {
            @Override
            public void close() throws IOException {
                // Empty close implementation for testing
            }
        });

        String result = mReader.toString();
        Assert.assertEquals(mockBlockReader.toString(), result);
    }
}
