package ppdz2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.ReentrantLock;

public class Philosophs {

    private static final class Fork {
        final int number;
        final ReentrantLock mutex = new ReentrantLock(true);

        Fork(int number) {
            this.number = number;
        }
    }

    private static final class DiningRoom {
        private final Fork[] forks;
        private final AtomicIntegerArray users;
        private final AtomicBoolean error = new AtomicBoolean();

        DiningRoom(int count) {
            forks = new Fork[count];
            users = new AtomicIntegerArray(count);

            for (int i = 0; i < count; i++) {
                forks[i] = new Fork(i);
            }
        }

        void haveDinner(int philosopher) {
            Fork a = forks[philosopher];
            Fork b = forks[(philosopher + 1) % forks.length];
            // Вилки блокируются по возрастанию номера.
            Fork first = a.number < b.number ? a : b;
            Fork second = a.number < b.number ? b : a;

            first.mutex.lock();
            try {
                second.mutex.lock();
                try {
                    useForks(philosopher, a, b);
                } finally {
                    second.mutex.unlock();
                }
            } finally {
                first.mutex.unlock();
            }
        }

        private void useForks(int philosopher, Fork left, Fork right) {
            int leftCount = users.incrementAndGet(left.number);
            int rightCount = users.incrementAndGet(right.number);

            if (leftCount > 1 || rightCount > 1) {
                error.set(true);
            }

            try {
                Thread.sleep(1);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            } finally {
                users.decrementAndGet(right.number);
                users.decrementAndGet(left.number);
            }
        }

        boolean hasConflict() {
            return error.get();
        }
    }

    public static void main(String[] args) throws Exception {
        final int count = 5;
        final int dinners = 100;

        DiningRoom room = new DiningRoom(count);
        ExecutorService executor = Executors.newFixedThreadPool(count);
        List<Future<Integer>> tasks = new ArrayList<>();

        try {
            for (int id = 0; id < count; id++) {
                final int philosopher = id;

                tasks.add(executor.submit(() -> {
                    int completed = 0;

                    while (completed < dinners) {
                        room.haveDinner(philosopher);
                        completed++;
                        Thread.yield();
                    }

                    return completed;
                }));
            }

            for (int id = 0; id < tasks.size(); id++) {
                int completed = tasks.get(id).get(10, TimeUnit.SECONDS);

                if (completed != dinners) {
                    throw new AssertionError(
                            "Philosopher " + id + " completed " + completed + " dinners"
                    );
                }
            }

        } catch (TimeoutException ex) {
            throw new AssertionError(
                    "Execution took too long: possible deadlock", ex
            );
        } finally {
            executor.shutdownNow();
        }

        if (room.hasConflict()) {
            throw new AssertionError("A fork was used by more than one philosopher");
        }

        System.out.println(
                "OK: " + dinners + " dinners per philosopher; no deadlock or fork conflict"
        );
    }
}

/*
 * 1. Нет, если ресурсы блокируются в одном порядке — дедлок невозможен.
 * 2. Да, освобождение ресурсов в том же порядке само по себе не предотвращает дедлок.
*/