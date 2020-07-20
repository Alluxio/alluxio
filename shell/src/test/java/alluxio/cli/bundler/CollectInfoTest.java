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

package alluxio.cli.bundler;

import static org.junit.Assert.assertEquals;

import alluxio.cli.bundler.command.AbstractCollectInfoCommand;
import alluxio.cli.bundler.command.CollectLogCommand;
import alluxio.conf.InstancedConfiguration;
import alluxio.cli.Command;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;

import org.apache.commons.lang.time.DateUtils;
import org.apache.log4j.Appender;
import org.junit.Test;
import org.reflections.Reflections;

import java.lang.reflect.Modifier;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.Locale;

public class CollectInfoTest {
  private static InstancedConfiguration sConf =
          new InstancedConfiguration(ConfigurationUtils.defaults());

  private int getNumberOfCommands() {
    Reflections reflections =
            new Reflections(AbstractCollectInfoCommand.class.getPackage().getName());
    int cnt = 0;
    for (Class<? extends Command> cls : reflections.getSubTypesOf(Command.class)) {
      if (!Modifier.isAbstract(cls.getModifiers())) {
        cnt++;
      }
    }
    return cnt;
  }

  @Test
  public void loadedCommands() {
    CollectInfo ic = new CollectInfo(sConf);
    Collection<Command> commands = ic.getCommands();
    assertEquals(getNumberOfCommands(), commands.size());
  }

  @Test
  public void loggers() {
    org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();
    Enumeration e = rootLogger.getAllAppenders();
    while (e.hasMoreElements()) {
      Appender app = (Appender) e.nextElement();
      System.out.println(app.getName());
      System.out.println(app);
    }
    System.out.println(rootLogger.getAppender("JOB_MASTER_LOGGER"));

  }

  @Test
  public void getDateTime() throws Exception {
    int len = "2020-03-19 11:57:58,883".length();
    String content = "2020-03-19 11:57:58,883 INFO  NettyUtils - EPOLL is not available, will use NIO\n" +
            "2020-03-19 11:57:58,963 INFO  ExtensionFactoryRegistry - Loading core jars from /Users/jiachengliu/Documents/Alluxio/alluxio-jiacheng/alluxio/lib\n" +
            "2020-03-19 11:57:58,997 INFO  TieredIdentityFactory - Initialized tiered identity TieredIdentity(node=localhost, rack=null)\n";
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS");
    LocalDate dateTime = LocalDate.parse(content.substring(0, len), dtf);
    System.out.println(dateTime);


    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS", Locale.ENGLISH);

    Date date = formatter.parse(content.substring(0, len));
    System.out.println(date);
  }

  @Test
  public void parseGCLog() throws Exception {
    int len = "2020-06-23T23:52:23.970+0800".length();
    String content = "2020-05-16T00:00:01.084+0800\tINFO\tdispatcher-query-7960\tcom.bluetalon.presto.btclient.BtTcpClientPoolFactory\tAttempting to connect to PDP";
//    Instant i = Instant.parse(content);
//    Date d = Date.from(i);
//    System.out.println(d);
//    DateTimeFormatter dtf = DateTimeFormatter.ISO_DATE_TIME;
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXX");
    LocalDateTime datetime = LocalDateTime.parse(content.substring(0, len), dtf);
    System.out.println(datetime);
//    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-ddTHH:mm:ssXX", Locale.ENGLISH);

//    Date date = formatter.parse(content.substring(0, len));
//    System.out.println(d.format(dtf));
  }

  @Test
  public void parseDate() {
    String[] contents = new String[]{
            "2020-05-15 09:21:52,359 INFO BlockStateChange: BLOCK* addStoredBlock: blockMap updated: 10.70.22.120:1025 is added to blk_1126110055_52485351{blockUCState=UNDER_CONSTRUCTION, primaryNodeIndex=-1, replicas=[ReplicaUnderConstruction[[DISK]DS-f2c7ade9-525a-4364-84c1-1d62bd3bf2b2:NORMAL:10.70.22.120:1025|FINALIZED]]} size 0",
            "20/05/18 16:11:18 INFO util.SignalUtils: Registered signal handler for TERM",
            "2020-05-16T00:00:01.084+0800\tINFO\tdispatcher-query-7960\tcom.bluetalon.presto.btclient.BtTcpClientPoolFactory\tAttempting to connect to PDP",
            "2020-05-14 21:05:53,822 WARN org.apache.zookeeper.server.NIOServerCnxn: caught end of stream exception",
            "2020-06-27 11:58:53 INFO  SignalUtils:57 - Registered signal handler for INT",
            "2020-06-23T23:52:23.970+0800: 1.652: [CMS-concurrent-mark-start]",
            "2020-03-19 11:57:58,997 INFO  TieredIdentityFactory - Initialized tiered identity TieredIdentity(node=localhost, rack=null)\n"
    };

    for (int i = 0; i < contents.length; i++) {
      String s = contents[i];
      LocalDateTime datetime = CollectLogCommand.parseDateTime(s);
      System.out.println(datetime);
    }
  }
}
