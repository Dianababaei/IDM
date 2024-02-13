package ir.sharif.math.ap2023.hw5;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MultiThreadCopier {
    public static long SAFE_MARGIN = 6;
    private final List<long[]> segments;
    private final String dest;
    private final SourceProvider sourceProvider;
    private final long hence;
    private final int workerCount;

    public MultiThreadCopier(SourceProvider sourceProvider, String dest, int workerCount) {
        this.workerCount = workerCount;
        this.dest = dest;
        this.sourceProvider = sourceProvider;
        this.hence = sourceProvider.size();
        segments = Collections.synchronizedList(new ArrayList<long[]>());
    }
    public void start() {
        try {
            RandomAccessFile file = new RandomAccessFile(dest, "rw");
            file.setLength(hence);
            file.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        for (int i = 0; i < workerCount; i++) {
            long[] segment = new long []{i * (hence / workerCount), (i + 1) * (hence / workerCount)};
            if (i == workerCount - 1) segment[1] = (hence);
            segments.add(segment);
        }

        MyWriter[] myWriters = new MyWriter[workerCount];
        for (int i = 0; i < workerCount; i++)
            myWriters[i] = new MyWriter(i);

        for (int i = 0; i < workerCount; i++)
            myWriters[i].start();
    }
    private class MyWriter extends Thread {
        private final int workerIndex;
        private final RandomAccessFile fileWriter;

        public MyWriter(int workerIndex) {
            this.workerIndex = workerIndex;
            try {
                fileWriter = new RandomAccessFile(dest, "rw");
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        public boolean updateResponsibility() {
            synchronized (segments) {
                long max = -1;
                int index = 0;

                for (int i = 0; i < segments.size(); i++) {
                    long[]  segment = segments.get(i);
                    if (segment[1] - segment[0] > max) {
                        max = segment[1] - segment[0];
                        index = i;
                    }
                }

                if (max >= SAFE_MARGIN) {
                    long[] segment = segments.get(index);
                    long mid = (segment[1] + segment[0]) / 2;
                    segments.set(workerIndex, new long[] {mid, segment[1]});
                    segment[1] = (mid);
                    return true;
                }
                return false;
            }
        }


        @Override
        public void run() {
            super.run();
            do {
                SourceReader sourceReader = sourceProvider.connect(segments.get(workerIndex)[0]);
                try {
                    fileWriter.seek(segments.get(workerIndex)[0]);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                while (segments.get(workerIndex)[0] < segments.get(workerIndex)[1]) {
                    try {
                        synchronized (segments) {
                            segments.get(workerIndex)[0] = (segments.get(workerIndex)[0] + 1);
                        }
                        fileWriter.write(sourceReader.read());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

            } while (updateResponsibility());

            try {
                fileWriter.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}