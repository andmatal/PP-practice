public class Mandelbrot {
    private final int WIDTH = 100;     // ширина картинки
    private final int HEIGHT = 40;     // высота картинки
    private final int MAX_ITER = 100;  // максимум итераций
    private final int THREADS = 6;     // сколько потоков запустить


    private final double X_MIN = -2.5, X_MAX = 1.0;
    private final double Y_MIN = -1.25, Y_MAX = 1.25;

    private final int[][] result = new int[HEIGHT][WIDTH];

    private int countIterations(double cx, double cy) {
        double zx = 0, zy = 0;
        int i = 0;
        // |z| <= 2  <=>  zx^2 + zy^2 <= 4
        while (i < MAX_ITER && zx * zx + zy * zy <= 4.0) {
            double tmp = zx * zx - zy * zy + cx;
            zy = 2 * zx * zy + cy;
            zx = tmp;
            i++;
        }
        return i;
    }

    private class Worker implements Runnable {
        int yFrom, yTo;

        Worker(int yFrom, int yTo) {
            this.yFrom = yFrom;
            this.yTo = yTo;
        }

        public void run() {
            double dx = (X_MAX - X_MIN) / WIDTH;
            double dy = (Y_MAX - Y_MIN) / HEIGHT;

            for (int y = yFrom; y < yTo; y++) {
                double cy = Y_MIN + y * dy;
                for (int x = 0; x < WIDTH; x++) {
                    double cx = X_MIN + x * dx;
                    result[y][x] = countIterations(cx, cy);
                }
            }
        }
    }

    public void run() throws InterruptedException {

        long start = System.currentTimeMillis();

        Thread[] threads = new Thread[THREADS];
        int rowsPerThread = HEIGHT / THREADS;

        for (int i = 0; i < THREADS; i++) {
            int yFrom = i * rowsPerThread;
            int yTo = (i == THREADS - 1) ? HEIGHT : (i + 1) * rowsPerThread;
            threads[i] = new Thread(new Worker(yFrom, yTo));
        }

        for (Thread t : threads) {
            t.start();
        }

        for (Thread t : threads) {
            t.join();
        }

        long time = System.currentTimeMillis() - start;
        System.out.println("Посчитано за " + time + " мс\n");

        String chars = " .:-=+*#%@";
        for (int y = 0; y < HEIGHT; y++) {
            for (int x = 0; x < WIDTH; x++) {
                int iter = result[y][x];
                if (iter == MAX_ITER) {
                    System.out.print("@");
                } else {
                    int idx = (iter * (chars.length() - 1)) / MAX_ITER;
                    System.out.print(chars.charAt(idx));
                }
            }
            System.out.println();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        new Mandelbrot().run();
    }
}
