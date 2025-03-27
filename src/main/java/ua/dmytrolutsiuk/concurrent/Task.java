package ua.dmytrolutsiuk.concurrent;

public record Task(int id, int delay) {

    @Override
    public String toString() {
        return "Task{" +
                "id=" + id +
                ", delay=" + delay +
                '}';
    }
}
