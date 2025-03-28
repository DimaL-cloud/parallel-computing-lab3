package ua.dmytrolutsiuk;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import ua.dmytrolutsiuk.concurrent.FixedThreadPool;
import ua.dmytrolutsiuk.concurrent.Task;

import java.util.Random;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

@Slf4j
public class Main {

    private static final int WORKER_THREADS_AMOUNT = 6;
    private static final int QUEUE_SIZE = 15;
    private static final int TASK_GENERATORS_AMOUNT = 5;
    private static final int TASK_GENERATOR_MAX_DELAY = 1;
    private static final int MIN_TASK_DURATION = 5;
    private static final int MAX_TASK_DURATION = 10;
    private static final AtomicInteger nextTaskId = new AtomicInteger(1);

    @SneakyThrows
    public static void main(String[] args) {
        var threadPool = new FixedThreadPool(WORKER_THREADS_AMOUNT, QUEUE_SIZE);
        var taskGenerators = IntStream.range(0, TASK_GENERATORS_AMOUNT)
                .mapToObj(i -> new Thread(() -> {
                    while (true) {
                        var random = new Random();
                        int delay = MIN_TASK_DURATION + random.nextInt(MAX_TASK_DURATION - MIN_TASK_DURATION + 1);
                        int taskId = nextTaskId.getAndIncrement();
                        var task = new Task(taskId, delay);
                        try {
                            boolean submitted = threadPool.submit(task);
                            if (submitted) {
                                log.debug("Task {} submitted", task);
                            } else {
                                log.debug("Task {} rejected", task);
                            }
                        } catch (RejectedExecutionException e) {
                            log.error("Task {} rejected", task, e);
                            return;
                        }
                        try {
                            Thread.sleep(random.nextInt(TASK_GENERATOR_MAX_DELAY + 1) * 1000);
                        } catch (InterruptedException e) {
                            log.error("Task generator interrupted", e);
                            return;
                        }
                    }
                }))
                .toList();
        taskGenerators.forEach(Thread::start);

        Thread.sleep(15 * 1000);
        log.info("Stats: {}", threadPool.getStats());
    }
}