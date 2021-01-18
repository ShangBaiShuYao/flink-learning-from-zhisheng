package com.shangbaishuyao.common.model;

/**
 *@Author: 上白书妖
 *@Date: 2020/11/13 11:51
 */
public class WordEvent {
    private String word;
    private int count;
    private long timestamp;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
