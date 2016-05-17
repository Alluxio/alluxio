package alluxio.heartbeat;

import alluxio.util.CommonUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;


/**
 * Created by shayne on 5/17/16.
 *
 * Unit tests for {@link SleepingTimer}.
 *
 * 1. Sleep more than the interval of SleepingTimer and see if the SleepingTimer warns correctly
 * 2. Tick continuously for several time and see if the time interval is correct
 * 3. Sleep less than the interval of SleepingTimer and see if the time interval is correct
 */

@RunWith(PowerMockRunner.class)
@PrepareForTest({SleepingTimer.class})
public class SleepingTimerTest {
    private static final String THREAD_NAME = "sleepingtimer-test-thread-name";
    private static final long INTERVAL_MS = 500;

    @Test
    public void test() throws Exception {
        SleepingTimer stimer =new SleepingTimer(THREAD_NAME, INTERVAL_MS);

        Logger logger= Mockito.mock(Logger.class);
        long PreTickMs = 0;

        Whitebox.setInternalState(SleepingTimer.class,"LOG",logger);

        Whitebox.setInternalState(stimer,"mPreviousTickMs",PreTickMs);


        stimer.tick();
        CommonUtils.sleepMs(5 * INTERVAL_MS);
        stimer.tick();

        Mockito.verify(logger).warn(Mockito.anyString(), Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong());

        long timeBeforeMs = PreTickMs;
        stimer.tick();
        stimer.tick();
        stimer.tick();
        long timeIntervalMs = System.currentTimeMillis() - timeBeforeMs;
        Assert.assertTrue(timeIntervalMs >= 3 * INTERVAL_MS);



        timeBeforeMs = PreTickMs;
        CommonUtils.sleepMs(INTERVAL_MS / 2);
        stimer.tick();
        timeIntervalMs = System.currentTimeMillis() - timeBeforeMs;
        Assert.assertTrue(timeIntervalMs >= INTERVAL_MS);

    }
}
