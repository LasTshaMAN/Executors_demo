import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class SchedulerTest {

    private static final int DELTA_IN_NANO_SECONDS = 100000000;

    @Rule
    public Timeout timeout = new Timeout(5, TimeUnit.SECONDS);

    private AtomicInteger amountOfFinishedTasks;
    private List<String> actualFinishingOrder;

    @Before
    public void setUp() throws Exception {
        this.amountOfFinishedTasks = new AtomicInteger(0);
        this.actualFinishingOrder = Collections.synchronizedList(new ArrayList<>());
    }

    @Test
    public void shouldScheduleTasksForExecutionInProperOrder() throws Exception {
        LocalDateTime firstTaskStartTime = LocalDateTime.now().plusNanos(DELTA_IN_NANO_SECONDS);
        LocalDateTime secondTaskStartTime = firstTaskStartTime.plusNanos(DELTA_IN_NANO_SECONDS);
        LocalDateTime thirdTaskStartTime = secondTaskStartTime.plusNanos(DELTA_IN_NANO_SECONDS);

        Scheduler<Void> scheduler = new Scheduler<>();

        scheduler.scheduleTask(thirdTaskStartTime, () -> {
            actualFinishingOrder.add("thirdTask");
            amountOfFinishedTasks.incrementAndGet();
            return null;
        });
        scheduler.scheduleTask(firstTaskStartTime, () -> {
            actualFinishingOrder.add("firstTask");
            amountOfFinishedTasks.incrementAndGet();
            return null;
        });
        scheduler.scheduleTask(secondTaskStartTime, () -> {
            actualFinishingOrder.add("secondTask");
            amountOfFinishedTasks.incrementAndGet();
            return null;
        });

        waitUntilAllTasksFinish(amountOfFinishedTasks, 3);

        List<String> expectedFinishingOrder = new ArrayList<>();
        expectedFinishingOrder.add("firstTask");
        expectedFinishingOrder.add("secondTask");
        expectedFinishingOrder.add("thirdTask");
        assertEquals(expectedFinishingOrder, actualFinishingOrder);
    }

    @Test
    public void shouldExecuteTasksFromPastImmediately() throws Exception {
        LocalDateTime pastTime = LocalDateTime.now().plusNanos(DELTA_IN_NANO_SECONDS);
        LocalDateTime futureTime = pastTime.plusNanos(2 * DELTA_IN_NANO_SECONDS);

        Scheduler<Void> scheduler = new Scheduler<>();

        scheduler.scheduleTask(futureTime, () -> {
            actualFinishingOrder.add("futureTask");
            amountOfFinishedTasks.incrementAndGet();
            return null;
        });
        scheduler.scheduleTask(pastTime, () -> {
            actualFinishingOrder.add("pastTask");
            amountOfFinishedTasks.incrementAndGet();
            return null;
        });

        waitUntilAllTasksFinish(amountOfFinishedTasks, 2);

        List<String> expectedFinishingOrder = new ArrayList<>();
        expectedFinishingOrder.add("pastTask");
        expectedFinishingOrder.add("futureTask");
        assertEquals(expectedFinishingOrder, actualFinishingOrder);
    }

    @Test
    public void shouldExecuteTasksInSubmissionOrderIfTheyAreScheduledToSameTime() throws Exception {
        LocalDateTime sameTime = LocalDateTime.now().plusNanos(DELTA_IN_NANO_SECONDS);

        Scheduler<Void> scheduler = new Scheduler<>();

        scheduler.scheduleTask(sameTime, () -> {
            actualFinishingOrder.add("firstTask");
            amountOfFinishedTasks.incrementAndGet();
            return null;
        });
        scheduler.scheduleTask(sameTime, () -> {
            actualFinishingOrder.add("secondTask");
            amountOfFinishedTasks.incrementAndGet();
            return null;
        });

        waitUntilAllTasksFinish(amountOfFinishedTasks, 2);

        List<String> expectedFinishingOrder = new ArrayList<>();
        expectedFinishingOrder.add("firstTask");
        expectedFinishingOrder.add("secondTask");
        assertEquals(expectedFinishingOrder, actualFinishingOrder);
    }

    private void waitUntilAllTasksFinish(AtomicInteger amountOfFinishedTasks, int totalAmountOfTasks) {
        long sleepIntervalMs = 100;
        while (amountOfFinishedTasks.get() < totalAmountOfTasks) {
            try {
                Thread.sleep(sleepIntervalMs);

            } catch (InterruptedException e) {
                // Ignore
            }
        }
    }
}