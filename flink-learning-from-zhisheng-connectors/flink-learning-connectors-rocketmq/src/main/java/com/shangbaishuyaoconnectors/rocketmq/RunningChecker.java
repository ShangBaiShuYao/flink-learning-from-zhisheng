package com.shangbaishuyaoconnectors.rocketmq;

import java.io.Serializable;

/**
 * Desc:
 * create by shangbaishuyao on 2020-01-24
 * @Author: 上白书妖
 * @Date: 14:48 2021/1/24
 */
public class RunningChecker implements Serializable {

    private volatile boolean isRunning =false;

    public boolean isRunning() {
        return isRunning;
    }

    public void setRunning(boolean running) {
        isRunning = running;
    }
}
