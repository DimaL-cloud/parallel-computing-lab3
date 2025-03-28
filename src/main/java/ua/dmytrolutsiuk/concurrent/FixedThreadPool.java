package ua.dmytrolutsiuk.concurrent;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;

@Slf4j
public class FixedThreadPool implements ThreadPool {

    private static final int NANO_TO_MILLIS_CONVERSION = 1_000_000;

    private final int queueSize;
    private final Queue<Task> taskQueue = new LinkedList<>();
    private final List<Worker> workerThreads = new ArrayList<>();
    private final Lock lock = new ReentrantLock();
    private final Condition notPausedCondition = lock.newCondition();
    private final Condition notEmptyCondition = lock.newCondition();
    private final AtomicInteger rejectedTasksAmount = new AtomicInteger(0);

    private ThreadPoolStatus status = ThreadPoolStatus.RUNNING;
    private boolean queueCurrentlyFull = false;
    private long queueFullStartTime = 0L;
    private long maxQueueFullTimeNanos = Long.MIN_VALUE;
    private long minQueueFullTimeNanos = Long.MAX_VALUE;

    public FixedThreadPool(int workerThreadsAmount, int queueSize) {
        this.queueSize = queueSize;
        IntStream.range(0, workerThreadsAmount)
                .mapToObj(Worker::new)
                .forEach(worker -> {
                    workerThreads.add(worker);
                    worker.start();
                });
    }

    @Override
    public boolean submit(Task task) {
        lock.lock();
        try {
            if (status == ThreadPoolStatus.SHUTDOWN) {
                throw new RejectedExecutionException("Thread pool is shut down");
            }
            if (taskQueue.size() >= queueSize) {
                rejectedTasksAmount.incrementAndGet();
                return false;
            }
            taskQueue.offer(task);
            if (taskQueue.size() == queueSize) {
                queueCurrentlyFull = true;
                queueFullStartTime = System.nanoTime();
            }
            notEmptyCondition.signal();
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void shutdown() {
        lock.lock();
        try {
            status = ThreadPoolStatus.SHUTDOWN;
            notEmptyCondition.signalAll();
        } finally {
            lock.unlock();
        }
        workerThreads.forEach(worker -> {
            try {
                worker.join();
            } catch (InterruptedException e) {
                log.debug("Error while waiting for worker to finish", e);
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void shutdownNow() {
        lock.lock();
        try {
            status = ThreadPoolStatus.SHUTDOWN;
            taskQueue.clear();
            notEmptyCondition.signalAll();
        } finally {
            lock.unlock();
        }
        workerThreads.forEach(Thread::interrupt);
    }

    @Override
    public void pause() {
        lock.lock();
        try {
            if (status == ThreadPoolStatus.RUNNING) {
                status = ThreadPoolStatus.PAUSED;
                log.debug("Thread pool paused");
            } else {
                log.debug("Can't pause thread pool, current status is {}", status);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void resume() {
        lock.lock();
        try {
            if (status == ThreadPoolStatus.PAUSED) {
                status = ThreadPoolStatus.RUNNING;
                notPausedCondition.signalAll();
                log.debug("Thread pool resumed");
            } else {
                log.debug("Can't resume thread pool, current status is {}", status);
            }
        } finally {
            lock.unlock();
        }
    }

    private class Worker extends Thread {

        private final int id;
        private final List<Long> totalWaitingTimes = new ArrayList<>();

        public Worker(int id) {
            this.id = id;
        }

        @Override
        public void run() {
            Task task;
            while (true) {
                lock.lock();
                try {
                    while ((taskQueue.isEmpty() || status == ThreadPoolStatus.PAUSED)
                            && status != ThreadPoolStatus.SHUTDOWN) {
                        try {
                            if (status == ThreadPoolStatus.PAUSED) {
                                notPausedCondition.await();
                            } else {
                                long startTime = System.nanoTime();
                                notEmptyCondition.await();
                                long endTime = System.nanoTime();
                                totalWaitingTimes.add(endTime - startTime);
                            }
                        } catch (InterruptedException e) {
                            log.debug("Interrupted while waiting for signal", e);
                            return;
                        }
                    }
                    if (status == ThreadPoolStatus.SHUTDOWN) {
                        return;
                    }
                    task = taskQueue.poll();
                    if (queueCurrentlyFull && taskQueue.size() == queueSize - 1) {
                        long durationNanos = System.nanoTime() - queueFullStartTime;
                        if (durationNanos > maxQueueFullTimeNanos) {
                            maxQueueFullTimeNanos = durationNanos;
                        }
                        if (durationNanos < minQueueFullTimeNanos) {
                            minQueueFullTimeNanos = durationNanos;
                        }
                        queueCurrentlyFull = false;
                    }
                } finally {
                    lock.unlock();
                }
                if (task != null) {
                    log.debug("Worker with id: {} started task {}", id, task);
                    try {
                        Thread.sleep(task.delay() * 1000L);
                    } catch (InterruptedException e) {
                        log.debug("Error while waiting for worker to finish", e);
                        return;
                    }
                    log.debug("Worker with id: {} finished task {}", id, task);
                }
            }
        }
    }

    public record Stats(
        int workerThreadsAmount,
        double averageWaitingTimeMills,
        double maxQueueFullTimeMills,
        double minQueueFullTimeMillis,
        int rejectedTasksAmount
    ) {
        @Override
        public String toString() {
            return "Stats{" +
                    "workerThreadsAmount=" + workerThreadsAmount +
                    ", averageWaitingTimeMills=" + averageWaitingTimeMills +
                    ", maxQueueFullTimeMills=" + maxQueueFullTimeMills +
                    ", minQueueFullTimeMillis=" + minQueueFullTimeMillis +
                    ", rejectedTasksAmount=" + rejectedTasksAmount +
                    '}';
        }
    }

    public Stats getStats() {
        lock.lock();
        try {
            return new Stats(
                    workerThreads.size(),
                    getAverageWaitingTimeMillis(),
                    (double) maxQueueFullTimeNanos / NANO_TO_MILLIS_CONVERSION,
                    (double) minQueueFullTimeNanos / NANO_TO_MILLIS_CONVERSION,
                    rejectedTasksAmount.get()
            );
        } finally {
            lock.unlock();
        }
    }

    private double getAverageWaitingTimeMillis() {
        long totalWaitTime = 0;
        long totalTasks = 0;
        for (Worker worker : workerThreads) {
            totalWaitTime += worker.totalWaitingTimes.stream().mapToLong(Long::longValue).sum();
            totalTasks += worker.totalWaitingTimes.size();
        }
        return totalTasks == 0 ? 0 : (double) totalWaitTime / totalTasks / NANO_TO_MILLIS_CONVERSION;
    }

}
