/*
 *  Copyright 2021 Netflix, Inc.
 *  <p>
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *  <p>
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  <p>
 *  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.contribs.tasks.googlepubsub;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.core.ApiFuture;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.netflix.conductor.core.utils.Utils;
import org.apache.commons.lang3.StringUtils;
import com.google.cloud.pubsub.v1.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.netflix.conductor.common.metadata.tasks.TaskType.TASK_TYPE_GOOGLE_CLOUD_PUBSUB;


@Component(TASK_TYPE_GOOGLE_CLOUD_PUBSUB)
public class GooglePublishTask extends WorkflowSystemTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(GooglePublishTask.class);

    static final String REQUEST_PARAMETER_NAME = "google_pubsub_request";
    private static final String MISSING_REQUEST =
        "Missing google pubsub request. Task input MUST have a '" + REQUEST_PARAMETER_NAME
            + "' key with GooglePublishTask.Input as value. See documentation for GooglePublishTask for required input parameters";
    private static final String MISSING_TOPIC_ID = "Missing cloud pub sub topic. See documentation for GooglePublishTask for required input parameters";
    private static final String MISSING_GOOGLE_PUBSUB_VALUE = "Missing cloud pub sub value.  See documentation for GooglePublishTask for required input parameters";
    private static final String FAILED_TO_INVOKE = "Failed to invoke google pubsub task due to: ";

    private final ObjectMapper objectMapper;
    private final String requestParameter;
    private final GooglePubSubProducerManager producerManager;

    @Autowired
    public GooglePublishTask(GooglePubSubProducerManager clientManager, ObjectMapper objectMapper) {
        super(TASK_TYPE_GOOGLE_CLOUD_PUBSUB);
        this.requestParameter = REQUEST_PARAMETER_NAME;
        this.producerManager = clientManager;
        this.objectMapper = objectMapper;
        LOGGER.info("GooglePublishTask initialized.");
    }

    @Override
    public void start(Workflow workflow, Task task, WorkflowExecutor executor) {

        long taskStartMillis = Instant.now().toEpochMilli();
        task.setWorkerId(Utils.getServerId());
        Object request = task.getInputData().get(requestParameter);

        if (Objects.isNull(request)) {
            markTaskAsFailed(task, MISSING_REQUEST);
            return;
        }

        Input input = objectMapper.convertValue(request, Input.class);

        if (StringUtils.isBlank(input.getTopicId())) {
            markTaskAsFailed(task, MISSING_TOPIC_ID);
            return;
        }

        if (Objects.isNull(input.getValue())) {
            markTaskAsFailed(task, MISSING_GOOGLE_PUBSUB_VALUE);
            return;
        }

        try {
            Future<String> recordMessageIdFuture = googleCloudPublish(input);
            try {
                recordMessageIdFuture.get();
                if (isAsyncComplete(task)) {
                    task.setStatus(Task.Status.IN_PROGRESS);
                } else {
                    task.setStatus(Task.Status.COMPLETED);
                }
                long timeTakenToCompleteTask = Instant.now().toEpochMilli() - taskStartMillis;
                LOGGER.debug("Published message {}, Time taken {}", input, timeTakenToCompleteTask);

            } catch (ExecutionException ec) {
                LOGGER.error("Failed to invoke google pub sub task: {} - execution exception ", task.getTaskId(), ec);
                markTaskAsFailed(task, FAILED_TO_INVOKE + ec.getMessage());
            }
        } catch (Exception e) {
            LOGGER.error("Failed to invoke google pub sub task:{} for input {} - unknown exception", task.getTaskId(), input, e);
            markTaskAsFailed(task, FAILED_TO_INVOKE + e.getMessage());
        }
    }

    private void markTaskAsFailed(Task task, String reasonForIncompletion) {
        task.setReasonForIncompletion(reasonForIncompletion);
        task.setStatus(Task.Status.FAILED);
    }

    /**
     * @param input Kafka Request
     * @return Future for execution.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private Future<String> googleCloudPublish(Input input) throws Exception {

        long startPublishingEpochMillis = Instant.now().toEpochMilli();

        Publisher publisher = producerManager.getPublisher(input);

        long timeTakenToCreatePublisher = Instant.now().toEpochMilli() - startPublishingEpochMillis;

        LOGGER.debug("Time taken getting producer {}", timeTakenToCreatePublisher);

        String message = objectMapper.writeValueAsString(input.getValue());
        ByteString data = ByteString.copyFromUtf8(message);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

        // Once published, returns a server-assigned message id (unique within the topic)
        ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
        String messageId = messageIdFuture.get();
        LOGGER.debug("Published message ID: " + messageId);

        long timeTakenToPublish = Instant.now().toEpochMilli() - startPublishingEpochMillis;

        LOGGER.debug("Time taken publishing {}", timeTakenToPublish);

        return messageIdFuture;
    }

    @Override
    public boolean execute(Workflow workflow, Task task, WorkflowExecutor executor) {
        return false;
    }

    @Override
    public void cancel(Workflow workflow, Task task, WorkflowExecutor executor) {
        task.setStatus(Task.Status.CANCELED);
    }

    @Override
    public boolean isAsync() {
        return true;
    }


    public static class Input {

        private Map<String, Object> headers = new HashMap<>();
        private String topicId;
        private Object key;
        private Object value;

        public Map<String, Object> getHeaders() {
            return headers;
        }

        public void setHeaders(Map<String, Object> headers) {
            this.headers = headers;
        }

        public Object getKey() {
            return key;
        }

        public void setKey(Object key) {
            this.key = key;
        }

        public Object getValue() {
            return value;
        }

        public void setValue(Object value) {
            this.value = value;
        }

        public String getTopicId() {
            return topicId;
        }

        public void setTopicId(String topicId) {
            this.topicId = topicId;
        }

        @Override
        public String toString() {
            return "Input{" +
                "headers=" + headers +
                ", topicId='" + topicId + '\'' +
                ", key=" + key +
                ", value=" + value +
                '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Input input = (Input) o;
            return Objects.equals(headers, input.headers) && Objects.equals(topicId, input.topicId) && Objects.equals(key, input.key) && Objects.equals(value, input.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(headers, topicId, key, value);
        }
    }
}
