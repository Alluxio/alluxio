package logging.alluxio.list;

public class LoggingAspectBoundaries {
    public static final String whitelist = "execution(* alluxio..*(..))";
    public static final String blacklist = "!within(logging..*)";
    public static final String finish = "execution(* java.lang.System.exit(..))";
    public static final Long artificialThreadId = -1L;
}
