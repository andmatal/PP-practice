import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class WorkStealing {
    enum TaskDistribution { UNIFORM, PERIODIC, PARETO }
    enum Workload { SLEEP, CPU }

    interface Shutdownable { void shutdown(); }
    public interface ShutdownableExecutor extends Shutdownable, Executor {}

    static class FixedThreadPoolExecutor implements ShutdownableExecutor {
        private final java.util.concurrent.ExecutorService delegate;

        FixedThreadPoolExecutor(int threads) {
            delegate = Executors.newFixedThreadPool(threads);
        }

        @Override public void execute(Runnable command) {
            delegate.execute(command);
        }

        @Override public void shutdown() {
            delegate.shutdown();
            try {
                if (!delegate.awaitTermination(1, TimeUnit.DAYS)) {
                    delegate.shutdownNow();
                    throw new IllegalStateException("Исполнитель не завершился");
                }
            } catch (InterruptedException e) {
                delegate.shutdownNow();
                Thread.currentThread().interrupt();
                throw new RuntimeException("Поток прерван во время ожидания завершения исполнителя", e);
            }
        }
    }

    static class ThreadPerTaskExecutor implements ShutdownableExecutor {
        private final List<Thread> threads = new ArrayList<>();
        private boolean shuttingDown;

        @Override public synchronized void execute(Runnable command) {
            if (shuttingDown) throw new RejectedExecutionException("Исполнитель завершает работу");
            Thread t = new Thread(command);
            threads.add(t);
            t.start();
        }

        @Override public void shutdown() {
            List<Thread> snapshot;
            synchronized (this) {
                shuttingDown = true;
                snapshot = new ArrayList<>(threads);
            }
            for (Thread t : snapshot) {
                try { t.join(); }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }
    }

    static class RoundRobinExecutor implements ShutdownableExecutor {
        protected static final Runnable EXIT_TASK = () -> {};
        private final AtomicInteger counter = new AtomicInteger(0);
        protected final List<Thread> threads;
        protected final Vector<BlockingDeque<Runnable>> tasks;
        private volatile boolean isShuttingDown = false;

        RoundRobinExecutor(int threadCount) {
            if (threadCount <= 0) throw new IllegalArgumentException("Количество потоков должно быть положительным");
            Supplier<IntStream> stream = () -> IntStream.range(0, threadCount);
            tasks = new Vector<>(stream.get()
                    .mapToObj(x -> new LinkedBlockingDeque<Runnable>()).toList());
            threads = stream.get().mapToObj(this::spawnThread).toList();
            threads.forEach(Thread::start);
        }

        Thread spawnThread(int id) {
            return new Thread(() -> {
                while (true) {
                    Runnable task;
                    try { task = tasks.get(id).takeFirst(); }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    if (task == EXIT_TASK) return;
                    task.run();
                }
            }, "циклический-" + id);
        }

        @Override public void shutdown() {
            synchronized (this) {
                if (!isShuttingDown) {
                    isShuttingDown = true;
                    tasks.forEach(queue -> queue.addLast(EXIT_TASK));
                }
            }
            joinThreads();
        }

        protected void joinThreads() {
            for (Thread t : threads) {
                try { t.join(); }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }

        @Override public synchronized void execute(Runnable command) {
            if (isShuttingDown) throw new RejectedExecutionException("Исполнитель завершает работу");
            tasks.get(Math.floorMod(counter.getAndIncrement(), threads.size())).addLast(command);
        }
    }

    static class WorkStealingExecutor extends RoundRobinExecutor {
        WorkStealingExecutor(int threads) { super(threads); }

        @Override Thread spawnThread(int id) {
            return new Thread(() -> {
                while (true) {
                    Runnable task = tasks.get(id).pollFirst();
                    if (task == null) task = stealTask(id);
                    if (task == null) {
                        try { task = tasks.get(id).pollFirst(1, TimeUnit.MILLISECONDS); }
                        catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        if (task == null) continue;
                    }
                    if (task == EXIT_TASK) {
                        task = stealTask(id);
                        if (task == null) return;
                        tasks.get(id).addLast(EXIT_TASK);
                    }
                    task.run();
                }
            }, "ворующий-" + id);
        }

        private Runnable stealTask(int thiefId) {
            for (int offset = 1; offset < tasks.size(); offset++) {
                var queue = tasks.get((thiefId + offset) % tasks.size());
                var task = queue.pollLast();
                if (task == EXIT_TASK) {
                    task = queue.pollLast();
                    queue.addLast(EXIT_TASK);
                }
                if (task != null) return task;
            }
            return null;
        }
    }

    static final int THREAD_NUMBER = Math.max(2, Runtime.getRuntime().availableProcessors());
    static final int TASK_NUMBER = 100_000;
    static final int TARGET_OPTIMAL_FULL_TIME = 10_000;
    static final int MEAN_TASK_TIME = (int) Math.round((TARGET_OPTIMAL_FULL_TIME + 0d) / TASK_NUMBER * THREAD_NUMBER);
    static final int LOWER_TASK_TIME_BOUND = 0;
    static final int HIGHER_TASK_TIME_BOUND = MEAN_TASK_TIME * 2 + 1;
    static final long TARGET_TOTAL_TASK_TIME = (long) TARGET_OPTIMAL_FULL_TIME * THREAD_NUMBER;
    static final long RANDOM_SEED = 42;
    static final double PARETO_SHAPE = 1.5;
    static final int PARETO_RAW_MAX_TASK_TIME = Math.max(1, MEAN_TASK_TIME * 100);
    static volatile long blackHoleSink;

    static long blackHole(long difficulty) {
        long value = 0x9E3779B97F4A7C15L;
        for (long i = 0; i < difficulty; i++) {
            value ^= (value << 13);
            value ^= (value >>> 7);
            value ^= (value << 17);
            value += i * 0xBF58476D1CE4E5B9L;
        }
        blackHoleSink ^= value;
        return value;
    }

    static Runnable createTask(int duration, Workload workload) {
        return () -> {
            if (workload == Workload.SLEEP) {
                try { Thread.sleep(duration); }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            } else {
                // Приблизительно заменяем миллисекунды ожидания вычислениями на процессоре.
                // Настройте этот множитель под свой компьютер; это не откалиброванное преобразование времени в число итераций.
                blackHole((long) duration * 20_000L);
            }
        };
    }

    static int createTaskDuration(TaskDistribution distribution, int taskId, Random random) {
        return switch (distribution) {
            case UNIFORM -> random.nextInt(LOWER_TASK_TIME_BOUND, Math.max(LOWER_TASK_TIME_BOUND + 1, HIGHER_TASK_TIME_BOUND));
            case PERIODIC -> taskId % THREAD_NUMBER == 0 ? Math.max(1, MEAN_TASK_TIME * THREAD_NUMBER) : 0;
            case PARETO -> {
                double scale = Math.max(1, MEAN_TASK_TIME) * (PARETO_SHAPE - 1) / PARETO_SHAPE;
                double duration = scale / Math.pow(1 - random.nextDouble(), 1 / PARETO_SHAPE);
                yield (int) Math.min(Math.round(duration), PARETO_RAW_MAX_TASK_TIME);
            }
        };
    }

    static int[] createTaskDurations(TaskDistribution distribution) {
        var random = new Random(RANDOM_SEED);
        var durations = new int[TASK_NUMBER];
        long totalDuration = 0;
        for (int taskId = 0; taskId < TASK_NUMBER; taskId++) {
            durations[taskId] = createTaskDuration(distribution, taskId, random);
            totalDuration += durations[taskId];
        }
        if (totalDuration == 0) return durations;

        double scale = (double) TARGET_TOTAL_TASK_TIME / totalDuration;
        double remainder = 0;
        long normalizedTotal = 0;
        for (int taskId = 0; taskId < TASK_NUMBER; taskId++) {
            double scaledDuration = durations[taskId] * scale + remainder;
            durations[taskId] = (int) scaledDuration;
            remainder = scaledDuration - durations[taskId];
            normalizedTotal += durations[taskId];
        }
        for (int taskId = 0; normalizedTotal < TARGET_TOTAL_TASK_TIME; taskId++) {
            durations[taskId % TASK_NUMBER]++;
            normalizedTotal++;
        }
        for (int taskId = 0; normalizedTotal > TARGET_TOTAL_TASK_TIME; taskId++) {
            int index = taskId % TASK_NUMBER;
            if (durations[index] > 0) {
                durations[index]--;
                normalizedTotal--;
            }
        }
        return durations;
    }

    static Vector<Runnable> createTasks(TaskDistribution distribution, Workload workload) {
        var durations = createTaskDurations(distribution);
        return new Vector<>(IntStream.range(0, TASK_NUMBER)
                .mapToObj(x -> createTask(durations[x], workload)).toList());
    }

    public record Pair<A, B>(A first, B second) {}

    public static Pair<Long, Long> measureExecutor(ShutdownableExecutor executor,
                                                   TaskDistribution distribution,
                                                   Workload workload) {
        var tasks = createTasks(distribution, workload);
        long start = System.nanoTime();
        tasks.forEach(executor::execute);
        long submissionEnd = System.nanoTime();
        executor.shutdown();
        long finish = System.nanoTime();
        return new Pair<>(submissionEnd - start, finish - start);
    }

    static void printResult(String name, Pair<Long, Long> result) {
        System.out.printf("  %-24s отправка=%9.3f мс общее время=%10.3f мс%n",
                name, result.first() / 1_000_000d, result.second() / 1_000_000d);
    }

    static void benchmark(String name, Supplier<ShutdownableExecutor> factory,
                          TaskDistribution distribution, Workload workload) {
        System.out.println("\n" + name);
        printResult("Фиксированный пул", measureExecutor(factory.get(), distribution, workload));
    }

    public static void main(String[] args) {
        System.out.println("Количество потоков: " + THREAD_NUMBER + ", количество задач: " + TASK_NUMBER);
        System.out.println("Запускайте на компьютере без лишней нагрузки; повторите несколько раз и сравните медианные значения.");
        for (Workload workload : Workload.values()) {
            System.out.println("\n========== ТИП НАГРУЗКИ: " + (workload == Workload.SLEEP ? "ОЖИДАНИЕ" : "ВЫЧИСЛЕНИЯ") + " ==========");
            for (TaskDistribution distribution : TaskDistribution.values()) {
                System.out.println("\nРаспределение: " + switch (distribution) {
                    case UNIFORM -> "РАВНОМЕРНОЕ";
                    case PERIODIC -> "ПЕРИОДИЧЕСКОЕ";
                    case PARETO -> "ПАРЕТО";
                });
                printResult("Фиксированный пул",
                        measureExecutor(new FixedThreadPoolExecutor(THREAD_NUMBER), distribution, workload));
                printResult("Циклическое распределение",
                        measureExecutor(new RoundRobinExecutor(THREAD_NUMBER), distribution, workload));
                printResult("Кража задач",
                        measureExecutor(new WorkStealingExecutor(THREAD_NUMBER), distribution, workload));
                if (workload == Workload.SLEEP && distribution == TaskDistribution.UNIFORM) {
                    // Режим «поток на задачу» создаёт TASK_NUMBER потоков и может исчерпать ресурсы системы.
                    printResult("Поток на задачу",
                            measureExecutor(new ThreadPerTaskExecutor(), distribution, workload));
                }
            }
        }
        /*
         * Результаты:
         *
         * SLEEP:
         *
         * UNIFORM:
         * FixedThreadPool  - 12753,674 ms
         * RoundRobin       - 12664,801 ms
         * WorkStealing     - 12406,497 ms
         * ThreadPerTask    - 35121,495 ms
         *
         * Тут результаты у первых трёх executor'ов довольно похожие.
         * ThreadPerTask сильно медленнее, потому что создаёт очень много потоков.
         *
         * PERIODIC:
         * FixedThreadPool  - 10310,831 ms
         * RoundRobin       - 216306,830 ms
         * WorkStealing     - 10355,846 ms
         *
         * Здесь RoundRobin сильно проиграл. Скорее всего, длинные задачи
         * попали в одну очередь, поэтому один поток долго работал, а остальные
         * почти ничего не делали. WorkStealing смог это исправить.
         *
         * PARETO:
         * FixedThreadPool  - 12655,131 ms
         * RoundRobin       - 13419,742 ms
         * WorkStealing     - 12492,591 ms
         *
         * Здесь WorkStealing тоже показал лучший результат, но разница уже
         * небольшая.
         *
         * CPU (blackHole):
         *
         * UNIFORM:
         * FixedThreadPool  - 1333,616 ms
         * RoundRobin       - 1276,658 ms
         * WorkStealing     - 1165,375 ms
         *
         * WorkStealing оказался самым быстрым.
         *
         * PERIODIC:
         * FixedThreadPool  - 1077,438 ms
         * RoundRobin       - 10550,986 ms
         * WorkStealing     - 1087,961 ms
         *
         * Тут снова RoundRobin сильно хуже остальных из-за неравномерной
         * нагрузки. WorkStealing и FixedThreadPool показали почти одинаковый
         * результат.
         *
         * PARETO:
         * FixedThreadPool  - 1131,903 ms
         * RoundRobin       - 1146,128 ms
         * WorkStealing     - 1093,938 ms
         *
         * Здесь все результаты близкие, но WorkStealing снова оказался самым
         * быстрым.
         *
         * В целом WorkStealing лучше показывает себя на неравномерных задачах,
         * потому что потоки могут забирать задачи друг у друга. FixedThreadPool
         * тоже работает стабильно. RoundRobin может сильно проигрывать, когда
         * задачи распределяются неудачно.
         */
    }
}
