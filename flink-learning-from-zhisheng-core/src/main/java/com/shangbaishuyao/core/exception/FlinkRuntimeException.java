package com.shangbaishuyao.core.exception;

import java.io.Serializable;

/**
 * Desc: Flink的 RuntimeException(运行时异常) <br/>
 *       Base class of all Flink-specific unchecked exceptions <br/>
 *       是所有Flink-specific(具体的)未检查的异常的基类 <br/>
 *
 *@Author: 上白书妖
 *@Date: 2020/11/20 14:49
 */
public class FlinkRuntimeException extends RuntimeException implements Serializable {

    /**
     * Creates a new Exception with the given message and null as the cause.
     * 使用给定消息创建新的异常，并将null作为原因.
     *
     * @param message The exception message(消息异常)
     */
    public FlinkRuntimeException(String message){
        super(message);
    }


    /**
     * Creates a new exception with a null message and the given cause.
     * 创建带有空消息和给定原因的新异常
     *
     * @param cause The exception that caused this exception (引起此异常的异常)
     */
    //Throwable是java.lang包中一个专门用来处理异常的类。它有两个子类，即Error 和Exception，它们分别用来处理两组异常。
    public FlinkRuntimeException(Throwable cause){
        super(cause);
    }

    /**
     * Creates a new exception with the given message and cause.
     * 使用给定的消息和原因创建一个新的异常
     *
     * @param message The exception message (异常消息 )
     * @param cause The exception that caused this exception (引起此异常的异常 )
     */
    public FlinkRuntimeException(String message,Throwable cause){
        super(message,cause);
    }
}
