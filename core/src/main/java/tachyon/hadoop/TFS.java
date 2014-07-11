/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.hadoop;

import tachyon.Constants;

/**
 * An Hadoop FileSystem interface implementation. Any program working with Hadoop HDFS can work with
 * Tachyon transparently by using this class. However, it is not as efficient as using the Tachyon
 * API in tachyon.client package.
 */
public final class TFS extends AbstractTFS {

    @Override
    protected boolean isZookeeperMode() {
        return false;
    }

    @Override
    public String getScheme() {
        return Constants.SCHEME;
    }
}
