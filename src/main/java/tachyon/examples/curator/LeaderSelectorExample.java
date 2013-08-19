package tachyon.examples.curator;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;

import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

public class LeaderSelectorExample {
  private static final int CLIENT_QTY = 5;
  private static final String PATH = "/examples/leader";

  public static void main(String[] args) throws Exception {
    // all of the useful sample code is in ExampleClient.java

    System.out.println("Create " + CLIENT_QTY + " clients, have each negotiate for leadership "
        + "and then wait a random number of seconds before letting another leader election occur.");
    System.out.println("Notice that leader election is fair: all clients will become leader and "
        + "will do so the same number of times.");

    List<CuratorFramework> clients = Lists.newArrayList();
    List<ExampleClient> examples = Lists.newArrayList();
    TestingServer server = new TestingServer();
    try {
      System.out.println(server.getConnectString());
      for (int i = 0; i < CLIENT_QTY; ++i ) {
        CuratorFramework client = CuratorFrameworkFactory.newClient(
            server.getConnectString(), new ExponentialBackoffRetry(1000, 3));
        clients.add(client);

        ExampleClient example = new ExampleClient(client, PATH, "Client #" + i);
        examples.add(example);

        client.start();
        if (i == 4) {
          try {
            while (true) {
              if (client.checkExists().forPath("/leader") != null) {
                System.out.println("AAA " + client.getData().forPath("/leader").length + " " + new String(client.getData().forPath("/leader")));
                List<String> masters = client.getChildren().forPath("/leader");
                System.out.println("BBB " + client.getChildren().forPath("/leader"));
                long maxTime = 0;
                String leader = "";
                for (String s: masters) {
                  Stat stat = client.checkExists().forPath("/leader/" + s);
                  if (stat != null && stat.getCtime() > maxTime) {
                    maxTime = stat.getCtime();
                    leader = s;
                  }
                }

                System.out.println("CCC " + leader);
              } else {
                System.out.println("/leader" + " does not exist");
              }
              Thread.currentThread().sleep(1000);
            }
          } catch (Exception e) {
            e.printStackTrace();
            System.out.println("BAD*********************************");
          }
          //          System.out.println(client.isStarted());
          //          CuratorZookeeperClient zkClient = client.getZookeeperClient();
          //          try {
          //            zkClient.start();
          //            ZooKeeper zk = zkClient.getZooKeeper();
          //            System.out.println(zkClient.isConnected());
          //            System.out.println(zk.getChildren(PATH, false));
          //          } catch (Exception e) {
          //            e.printStackTrace();
          //            System.out.println("BAD");
          //          }
        } else {
          example.start();
        }
      }

      System.out.println("Press enter/return to quit\n");
      new BufferedReader(new InputStreamReader(System.in)).readLine();
    } finally {
      System.out.println("Shutting down...");

      for ( ExampleClient exampleClient : examples ) {
        Closeables.closeQuietly(exampleClient);
      }
      for ( CuratorFramework client : clients ) {
        Closeables.closeQuietly(client);
      }

      Closeables.closeQuietly(server);
    }
  }
}