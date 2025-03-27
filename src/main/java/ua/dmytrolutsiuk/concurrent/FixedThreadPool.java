package ua.dmytrolutsiuk.concurrent;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;

@Slf4j
public class FixedThreadPool implements ThreadPool {

    private final int queueSize;
    private final Queue<Task> taskQueue = new LinkedList<>();
    private final List<Worker> workerThreads = new ArrayList<>();
    private final Lock lock = new ReentrantLock();
    private final Condition notPausedCondition = lock.newCondition();
    private final Condition notEmptyCondition = lock.newCondition();

    private ThreadPoolStatus status = ThreadPoolStatus.RUNNING;

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
                return false;
            }
            taskQueue.offer(task);
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
                log.error("Error while waiting for worker to finish", e);
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
                log.info("Thread pool paused");
            } else {
                log.info("Can't pause thread pool, current status is {}", status);
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
                log.info("Thread pool resumed");
            } else {
                log.info("Can't resume thread pool, current status is {}", status);
            }
        } finally {
            lock.unlock();
        }
    }

    private class Worker extends Thread {

        private final int id;

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
                                notEmptyCondition.await();
                            }
                        } catch (InterruptedException e) {
                            log.error("Interrupted while waiting for signal", e);
                            return;
                        }
                    }
                    if (status == ThreadPoolStatus.SHUTDOWN) {
                        return;
                    }
                    task = taskQueue.poll();
                } finally {
                    lock.unlock();
                }
                if (task != null) {
                    log.info("Worker with id: {} started task {}", id, task);
                    try {
                        Thread.sleep(task.delay());
                    } catch (InterruptedException e) {
                        log.error("Error while waiting for worker to finish", e);
                        return;
                    }
                    log.info("Worker with id: {} finished task {}", id, task);
                }
            }
        }
    }
}
