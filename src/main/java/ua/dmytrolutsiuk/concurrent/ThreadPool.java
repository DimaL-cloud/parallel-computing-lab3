package ua.dmytrolutsiuk.concurrent;

public interface ThreadPool {

    boolean submit(Task task);

    void shutdown();

    void shutdownNow();

    void pause();

    void resume();
}
