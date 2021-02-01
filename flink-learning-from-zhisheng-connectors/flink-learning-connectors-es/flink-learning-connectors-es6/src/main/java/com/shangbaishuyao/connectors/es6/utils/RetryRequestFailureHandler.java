package com.shangbaishuyao.connectors.es6.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.util.ExceptionUtils;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Optional;

/**
 * Desc: es sink Request Failure Handler(es接收请求失败处理程序) <br/>
 * create by shangbaishuyao on 2021/2/1
 * @Author: 上白书妖
 * @Date: 15:22 2021/2/1
 *
 *
 * 用户提供了一个ActionRequestFailureHandler的实现来定义失败的动作请求应该如何处理，
 * 例如删除它们，重新处理格式错误的文档，或者只是请求再次发送到Elasticsearch，如果失败只是暂时的。
 * Example:
 *
 * 	private static class ExampleActionRequestFailureHandler implements ActionRequestFailureHandler {
 *
 *        @Override
 *        void onFailure(ActionRequest action, Throwable failure, int restStatusCode, RequestIndexer indexer) throws Throwable {
 * 			if (ExceptionUtils.findThrowable(failure, EsRejectedExecutionException.class).isPresent()) {
 * 				// full queue; re-add document for indexing
 * 				indexer.add(action);
 *            } else if (ExceptionUtils.findThrowable(failure, ElasticsearchParseException.class).isPresent()) {
 * 				// malformed document; simply drop request without failing sink
 *            } else {
 * 				// for all other failures, fail the sink;
 * 				// here the failure is simply rethrown, but users can also choose to throw custom exceptions
 * 				throw failure;
 *            }
 *        }
 *    }
 * 上面的示例将允许接收重新添加由于队列容量饱和而失败的请求，
 * 并删除带有格式错误的文档的请求，而不会使接收失败。
 * 对于所有其他的失败，sink将会失败。
 * 注意:对于Elasticsearch1.x，匹配失败的类型是不可行的，
 * 因为确切的类型不能通过旧版本的Java客户机api检索(因此，类型将是一般异常，仅在失败消息中有所不同)。
 * 在这种情况下，建议匹配所提供的REST状态代码。
 */
@Slf4j
public class RetryRequestFailureHandler implements ActionRequestFailureHandler {
    @Override
    public void onFailure(ActionRequest actionRequest, Throwable throwable, int restStatusCode, RequestIndexer requestIndexer) throws Throwable {
        if (ExceptionUtils.findThrowable(throwable, EsRejectedExecutionException.class).isPresent()) {
            requestIndexer.add(new ActionRequest[]{actionRequest});
        } else {
            if (ExceptionUtils.findThrowable(throwable, SocketTimeoutException.class).isPresent()) {
                return;
            } else {
                Optional<IOException> exp = ExceptionUtils.findThrowable(throwable, IOException.class);
                if (exp.isPresent()) {
                    IOException ioExp = exp.get();
                    if (ioExp != null && ioExp.getMessage() != null && ioExp.getMessage().contains("max retry timeout")) {
                        log.error(ioExp.getMessage());
                        return;
                    }
                }
            }
            throw throwable;
        }
    }
}
