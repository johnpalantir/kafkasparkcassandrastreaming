package com.palantir.KafkaSparkCassandraStreaming.streams.model;

import java.io.Serializable;

public class JobConfig implements Serializable {

    private boolean isLocal;
    private String topic;
    private String group;
    public JobConfig(){

    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public boolean isLocal() {
        return isLocal;
    }

    public void setLocal(boolean local) {
        isLocal = local;
    }
}
