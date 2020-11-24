package org.doit.source;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.Iterator;

public class MyFileExactlyParSource extends RichParallelSourceFunction<Tuple2<String, String>> implements CheckpointedFunction {
    private String path;
    private boolean flag = true;
    private transient ListState<Long> offsetState;
    private long offset = 0;

    public MyFileExactlyParSource() {
    }

    public MyFileExactlyParSource(String path) {
        this.path = path;
    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
        //获取offsetstate历史值，若offsetState为空，则是一个空的list,不是null
        Iterator<Long> iterator = offsetState.get().iterator();
        while (iterator.hasNext()) {
            offset = iterator.next();
        }
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        RandomAccessFile accessFile = new RandomAccessFile(path + "\\" + subtaskIndex + ".txt", "rw");
        //从指定的位置读取数据
        accessFile.seek(offset);
        //获取一个锁
        Object lock = ctx.getCheckpointLock();
        while (flag) {
            String line = accessFile.readLine();
            if (line != null) {
                line = new String(line.getBytes(), "utf-8");
                synchronized (lock) {
                    offset = accessFile.getFilePointer(); //第几个字节
                    ctx.collect(Tuple2.of(subtaskIndex + "", line));
                }
            } else {
                Thread.sleep(1000);
            }
        }


    }

    @Override
    public void cancel() {
        flag = false;
    }


    //定期将指定的状态数据保存到statebackend中
    //在checkppoint时会定期执行更新state到state backend
    //这个类存在线程不安全问题，比如一个线程在执行run方法，一个线程做快照了
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        //将历史值清楚
        offsetState.clear();
        //更新最新的状态值
        offsetState.add(offset);
        //这种方式错误的，往list里面放list
      //  offsetState.update(Collections.singletonList(offset));
    }


    //初始化OperatorState,生命周期方法，构造方法执行后执行一次
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        //定义一个状态描述器
        ListStateDescriptor<Long> stateDescriptor = new ListStateDescriptor<Long>(
                "offset-state",
                TypeInformation.of(new TypeHint<Long>() {
                })
                //Long.class
                //Types.Long
        );
        //初始化状态或获取历史状态(OperatorState)
        offsetState = context.getOperatorStateStore().getListState(stateDescriptor);

    }
}
