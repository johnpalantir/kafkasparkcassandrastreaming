package com.palantir.KafkaSparkCassandraStreaming.streams;

import com.palantir.KafkaSparkCassandraStreaming.streams.model.JobConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

public class ApiStreamingDataCollector implements Serializable {

    private final static Logger LOGGER = LoggerFactory.getLogger(ApiStreamingDataCollector.class);

    private JobConfig jobConfig;

    public ApiStreamingDataCollector(JobConfig jobConfig){
        this.jobConfig = jobConfig;
    }

    public static void main(String[] args) {

        try {
            JobConfig jobConfig = new JobConfig();
            jobConfig.setLocal(true);
            jobConfig.setGroup("myGroup");
            jobConfig.setTopic("palantir_raw_data");
            new ApiStreamingDataCollector(jobConfig).process();
        } catch (InterruptedException | IOException e) {
            LOGGER.info(e.getMessage());
            LOGGER.debug(e.getMessage(), e);
        }
    }

    private void process() throws InterruptedException, IOException{

        StreamingHelper.prepareRawDataReceiver(jobConfig);
    }

}
