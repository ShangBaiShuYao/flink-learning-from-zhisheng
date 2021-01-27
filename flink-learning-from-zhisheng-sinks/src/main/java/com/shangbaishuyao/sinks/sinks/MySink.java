package com.shangbaishuyao.sinks.sinks;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * Desc:  source ---channel----sink <br/>
 *
 *
 * 上白书妖补充: <br/>
 * <br>
 *     富函数（Rich Functions）:富函数和函数类是一样的,不过他有一个特点.他增加了生命周期的管理.什么叫生命周期?就是
 *     XXXFunction()什么时候初始化.什么时候销毁.什么时候执行你们的代码等就是所谓的生命周期的管理.
 *     XXXFunction()本身是没有生命周期的管理的.如果你需要增加生命周期的管理,你需要继承RichMapFunction()
 * <p>
 *     “富函数”是DataStream API提供的一个函数类的接口，所有Flink函数类都有其Rich版本。它与常规函数的不同在于，
 *     可以获取运行环境的上下文，并拥有一些生命周期方法，所以可以实现更复杂的功能。
 *     RichMapFunction
 *     RichFlatMapFunction
 *     RichFilterFunction
 *     …​
 *     Rich Function有一个生命周期的概念。典型的生命周期方法有：
 *     open()方法是rich function的初始化方法，当一个算子例如map或者filter被调用之前open()会被调用。
 *     close()方法是生命周期中的最后一个调用的方法，做一些清理工作。
 *     getRuntimeContext()方法提供了函数的RuntimeContext的一些信息，例如函数执行的并行度，任务的名字，以及state状态
 * <br/>
 *
 *
 *
 *<br>源码深入:<br/>
 *<br>①<br/>
 * public class MySink extends RichSinkFunction<String> {
 *
 * }
 *
 *<br>②<br/>
 * @Public
 * public abstract class RichSinkFunction<IN> extends AbstractRichFunction implements SinkFunction<IN> {
 *
 * 	private static final long serialVersionUID = 1L;
 * }
 *
 *  <br>解释:<br/>
 *      AbstractRichFunction:
 *           源码注释：
 *           An abstract stub implementation for rich user-defined functions.
 *           Rich functions have additional methods for initialization ({@link #open(Configuration)}) and
 *           teardown ({@link #close()}), as well as access to their runtime execution context via
 *           {@link #getRuntimeContext()}.
 *
 *           翻译：
 *           丰富的用户定义函数的抽象存根实现。
 *           富函数有额外的初始化方法({@link #open(Configuration)})和拆卸（关闭）方法({@link #close()})，
 *           以及通过访问它们的运行时执行上下文 {@link # getRuntimeContext()}。
 *
 *      SinkFunction<IN>:
 *            Interface for implementing user defined sink functionality.
 *            @param <IN> Input type parameter.
 *
 *           接口，用于实现用户定义的接收功能。
 *           @param <IN> 输入类型参数.
 *
 *
 *@Author: 上白书妖
 *@Date: 2020/11/24 15:05
 */
public class MySink extends RichSinkFunction<String> {
    private String tx;

    //构造方法
    public MySink(String tx) {
        System.out.println("=======================" + tx);
        this.tx = tx;
    }

    /**
     * Desc: open()方法是Rich function的初始化方法，当一个算子例如map或者filter被调用之前open()会被调用。
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        tx = "5";
        System.out.println("====================");
        super.open(parameters);
    }


    /**
     * Desc:  调用连接
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(String value, Context context) throws Exception {
        System.out.println(value + " " + tx);
    }
}
