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

package alluxio.cli.fs.command;

import alluxio.cli.Command;
import alluxio.cli.CommandReader;
import alluxio.cli.ConfigurationDocGenerator;
import alluxio.cli.fs.FileSystemShellUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.util.ConfigurationUtils;
import alluxio.util.io.PathUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.io.Closer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URL;
import java.util.Map;
import java.util.Set;

public class fsUnitTest{
    private static Map<String, Command> commands;

    @Before
    public void loadFsCommands(){
        Closer mCloser = Closer.create();
        commands = FileSystemShellUtils.loadCommands(mCloser.register(FileSystemContext.create(ServerConfiguration.global())));
    }

    @Test
    public void checkDocs() throws IOException {
        for(Map.Entry<String, Command> cmd : commands.entrySet()){
            Command c = cmd.getValue();
            Assert.assertNotNull(c.getCommandName());
            Assert.assertNotNull(c.getUsage());
            Assert.assertNotNull(c.getDescription());
            Assert.assertNotNull(c.getExample());
        }
    }
}