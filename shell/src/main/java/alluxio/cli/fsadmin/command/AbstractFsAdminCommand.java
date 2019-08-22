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

package alluxio.cli.fsadmin.command;

import alluxio.cli.Command;
import alluxio.cli.CommandReader;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.journal.JournalMasterClient;
import alluxio.client.meta.MetaMasterClient;
import alluxio.client.meta.MetaMasterConfigClient;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;

/**
 * Base class for fsadmin commands. It provides access to clients for talking to fs master, block
 * master, and meta master.
 */
public abstract class AbstractFsAdminCommand implements Command {
  protected final FileSystemMasterClient mFsClient;
  protected final BlockMasterClient mBlockClient;
  protected final MetaMasterClient mMetaClient;
  protected final MetaMasterConfigClient mMetaConfigClient;
  protected final PrintStream mPrintStream;
  protected final JournalMasterClient mMasterJournalMasterClient;
  protected final JournalMasterClient mJobMasterJournalMasterClient;

  private final static ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

  protected AbstractFsAdminCommand(Context context) {
    mFsClient = context.getFsClient();
    mBlockClient = context.getBlockClient();
    mMetaClient = context.getMetaClient();
    mMetaConfigClient = context.getMetaConfigClient();
    mMasterJournalMasterClient = context.getJournalMasterClientForMaster();
    mJobMasterJournalMasterClient = context.getJournalMasterClientForJobMaster();
    mPrintStream = context.getPrintStream();
  }

  protected URL getCommandFile(Class c){
    return c.getClassLoader().getResource(String.format("%s.yml", c.getSimpleName()));
  }

  @Override
  public String getUsage() {
    return getDocs().getUsage();
  }

  @Override
  public String getDescription() {
    return getDocs().getDescription();
  }

  @Override
  public String getExample() {
    return getDocs().getExample();
  }

  private String getSubCmd(){
    return getDocs().getSubCommands();
  }

  @Override
  public String getDocumentation() {
    if (getSubCmd() != null){
      return getDocs().toString() +
              "\nsubCommands: |\n  " + getSubCmd() + "\n";
    }
    else {
      return getDocs().toString()+ "\n";
    }
  }

  private CommandReader getDocs(){
    objectMapper.findAndRegisterModules();
    URL u = getCommandFile(this.getClass());
    try {
      return objectMapper.readValue(new File(u.getFile()), CommandReader.class);
    } catch (IOException e) {
      throw new RuntimeException("Could not get fsadmin command docs", e);
    }
  }
}
