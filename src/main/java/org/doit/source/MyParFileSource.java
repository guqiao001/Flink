package org.doit.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.RandomAccessFile;

public class MyParFileSource extends RichParallelSourceFunction<Tuple2<String, String>> {
    private String path;
    private boolean flag = true;

    public MyParFileSource() {
    }

    public MyParFileSource(String path) {
        this.path = path;
    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        RandomAccessFile accessFile = new RandomAccessFile(path + "\\" + subtaskIndex + ".txt", "rw");
        while (flag) {
            String line = accessFile.readLine();
            if (line != null) {
                line = new String(line.getBytes(), "utf-8");
                ctx.collect(Tuple2.of(subtaskIndex + "", line));
            } else {
                Thread.sleep(1000);
            }
        }


    }

    @Override
    public void cancel() {
        flag=false;
    }
}
