package logging.commands.list;

public class LoggingAspectBoundaries {
    public static final String whitelist = "execution(* alluxio..*(..))";
    public static final String blacklist = "!within(logging..*) && "
            + "!within(alluxio.clock..*) && "
            + "!within(alluxio.alluxio.client.metrics..*) && "
            + "!within(alluxio.heartbeat..*) && "
            + "!within(alluxio.resource..*) && "
            + "!within(alluxio.time..*) && "
            + "!within(alluxio.conf..*)";
    public static final String finish = "execution(* alluxio.security.authentication.onCompleted(..)) || "
             + "execution(* alluxio.cli.AbstractShell.close())";
    public static final Long artificialThreadId = -2L;
}
