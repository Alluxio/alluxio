package logging.commands.fs;

import br.com.simbiose.debug_log.BaseAspect;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static logging.commands.list.LoggingAspectBoundaries.*;

@Aspect
public class FsStartSyncCommandFlowLoggingAspect extends BaseAspect {

    private static final String FLOW_NAME = "FsStartSyncCommandFlow";

    private static final String START_METHOD = "execution(* alluxio.cli.fs.command.StartSyncCommand.run(..))";

    private static final String WHITE_AND_BLACK_LIST = whitelist + " && " + blacklist;

    private static final String FINISH_METHOD = finish;

    protected final Map<Long, Integer> threadIdToStep = new ConcurrentHashMap<>();
    protected final Map<Long, Long> threadIdToDebugLogId = new ConcurrentHashMap<>();

    /**
     * This method is executed when the Thread starts execution of creation schema command inside
     * worker.
     *
     * <p>It is necessary to create the step and debug log id variable for each thread that will
     * execute the flux. So it will delegate the processing to {@link
     * BaseAspect#printDebugLogForMethod(ProceedingJoinPoint, long)} that will be responsible to
     * insert inside the log information about method to be executed.
     *
     * @param point an object o Aspectj library that represents the method that ShannonDB needs to
     *              obtain information inside logs.
     * @return the same object that is returned by the method that is wrapped in this advice
     * @throws Throwable the same exception that is thrown by the method that is wrapped
     */
    @Around(START_METHOD)
    public Object startFlux(final ProceedingJoinPoint point) throws Throwable {
        final long threadId = artificialThreadId;

        threadIdToStep.put(threadId, 0);
        threadIdToDebugLogId.compute(
                threadId, (key, value) -> UUID.randomUUID().getMostSignificantBits());

        return printDebugLogForMethod(point, threadId);
    }

    /**
     * This method just retrieves the thread id and delegate the processing to method inside {@link
     * BaseAspect}
     *
     * <p>It will delegate the processing to {@link
     * BaseAspect#printDebugLogForMethod(ProceedingJoinPoint, long)} that will be responsible to
     * insert inside the log information about method to be executed.
     *
     * @param point an object o Aspectj library that represents the method that ShannonDB needs to
     *              obtain information inside logs.
     * @return the same object that is returned by the method that is wrapped in this advice
     * @throws Throwable the same exception that is thrown by the method that is wrapped
     */
    @Around(WHITE_AND_BLACK_LIST)
    public Object around(final ProceedingJoinPoint point) throws Throwable {
        final long threadId = artificialThreadId;

        return printDebugLogForMethod(point, threadId);
    }

    /**
     * This method is executed when the Thread starts execution when finishes the command of create
     * schema inside the worker.
     *
     * <p>It is necessary to remove the step and debug log id variable for each thread that already
     * executed the flux. It will first to delegate the processing to {@link
     * BaseAspect#printDebugLogForMethod(ProceedingJoinPoint, long)} that will be responsible to
     * insert inside the log information about method to be executed than it will reset information
     * about step and debug log id inside maps.
     *
     * @param point an object o Aspectj library that represents the method that ShannonDB needs to
     *              obtain information inside logs.
     * @return the same object that is returned by the method that is wrapped in this advice
     * @throws Throwable the same exception that is thrown by the method that is wrapped
     */
    @Around(FINISH_METHOD)
    public Object finishFlux(final ProceedingJoinPoint point) throws Throwable {
        final long threadId = artificialThreadId;

        final Object resultFromMethod = printDebugLogForMethod(point, threadId);

        threadIdToStep.remove(threadId);
        threadIdToDebugLogId.remove(threadId);

        return resultFromMethod;
    }

    /**
     * Defines a name for a general flux inside ShannonDB
     *
     * <p>The flow name is used inside logs because there are methods that are used in more than one
     * flux inside the code, so it is necessary to know the specific flux that call it.
     *
     * @return the name of a general flux inside ShannonDB.
     */
    @Override
    protected String getFlowName() {
        return FLOW_NAME;
    }

    /**
     * Defines a map of a thread to a step(or sequence) for a general flux inside ShannonDB
     *
     * <p>The step is a number that shows the sequence that methods are executed inside a flux.
     *
     * @return a map with a thread and and the order that a method is executed inside a flux.
     */
    @Override
    protected Map<Long, Integer> getThreadIdToStep() {
        return this.threadIdToStep;
    }

    /**
     * Defines a map of a thread to an identifier of flux's execution
     *
     * <p>Each time that a Thread execute a flux, a new debug log id is created and represents that
     * execution cycle.
     *
     * @return .
     */
    @Override
    protected Map<Long, Long> getThreadIdToDebugLogId() {
        return this.threadIdToDebugLogId;
    }
}
