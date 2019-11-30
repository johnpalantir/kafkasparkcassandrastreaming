package com.palantir.KafkaSparkCassandraStreaming.streams;

import java.io.Serializable;

public class Word implements Serializable {
    private static final long serialVersionUID = 1L;
    private String word;
    private int count;

    public  Word(){

    }
    public Word(String word, int count) {
        this.word = word;
        this.count = count;
    }
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

    @Override
    public String toString() {
        return "Word{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }
}