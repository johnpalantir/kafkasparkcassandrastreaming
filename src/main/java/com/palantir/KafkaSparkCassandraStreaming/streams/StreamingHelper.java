package com.palantir.KafkaSparkCassandraStreaming.streams;

import com.palantir.KafkaSparkCassandraStreaming.kafka.RawDataReceiver;
import com.palantir.KafkaSparkCassandraStreaming.streams.model.JobConfig;

public class StreamingHelper {
    public static RawDataReceiver prepareRawDataReceiver(JobConfig jobConfig) {
        RawDataReceiver receiver = new RawDataReceiver(
                jobConfig.getTopic(),
                jobConfig.getGroup(),
                jobConfig.isLocal());

        return receiver;
    }
}
