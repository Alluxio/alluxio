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

package alluxio.client.file.cache.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class FilterSpec
{
    private final List<DatabaseFilterSpec> databases;
    @JsonCreator
    public FilterSpec(@JsonProperty("databases") List<DatabaseFilterSpec> databases)
    {
        this.databases = requireNonNull(databases, "databases is null");
    }

    public List<DatabaseFilterSpec> getDatabases()
    {
        return databases;
    }
}