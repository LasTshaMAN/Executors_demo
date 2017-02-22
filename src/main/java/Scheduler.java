import java.time.LocalDateTime;
import java.util.concurrent.*;

/*
    This class implements a Task Scheduler

    Thread safe
 */
class Scheduler<TaskOutputType> {

    private final ScheduledThreadPoolExecutor executor;

    Scheduler() {
        this.executor = new ScheduledThreadPoolExecutor(1);
    }

    Future<TaskOutputType> scheduleTask(LocalDateTime executeAtThisTime, Callable<TaskOutputType> task) {
        int delayInNanoSeconds = executeAtThisTime.minusNanos(LocalDateTime.now().getNano()).getNano();
        return executor.schedule(task, delayInNanoSeconds, TimeUnit.NANOSECONDS);
    }
}
