package com.shangbaishuyao.demo.Function;

import com.shangbaishuyao.demo.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import java.util.Iterator;

public class MyMapFunc2 implements MapFunction<WaterSensor, Integer>, CheckpointedFunction {
    //定义状态
    private ListState<Integer> listState;
    private Integer count = 0;
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        listState = context
                .getOperatorStateStore()
                .getListState(new ListStateDescriptor<Integer>("state", Integer.class));

        Iterator<Integer> iterator = listState.get().iterator();

        while (iterator.hasNext()) {
            count += iterator.next();
        }
    }
    @Override
    public Integer map(WaterSensor value) throws Exception {
        count++;
        return count;
    }
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        listState.clear();
        listState.add(count);
    }
}
