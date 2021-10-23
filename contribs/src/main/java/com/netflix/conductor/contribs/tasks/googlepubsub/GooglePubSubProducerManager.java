/*
 * Copyright 2020 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.contribs.tasks.googlepubsub;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.CacheBuilder;
import com.google.pubsub.v1.TopicName;
import com.netflix.conductor.core.exception.ApplicationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("rawtypes")
@Component
public class GooglePubSubProducerManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(GooglePubSubProducerManager.class);

    private CredentialsProvider googleCredentialsProvider;
    private final String projectId;
    private final Cache<GooglePublishTask.Input, Publisher> googlePublisherProducerCache;

    private static final RemovalListener<GooglePublishTask.Input, Publisher> LISTENER = notification -> {
        if (notification.getValue() != null) {
            notification.getValue().shutdown();
            LOGGER.info("Closed producer for {}", notification.getKey());
        }
    };

    @Autowired
    public GooglePubSubProducerManager(@Value("${conductor.tasks.google-pubsub-publish.googleApplicationCredentialsPath:}") String googleApplicationCredentialsPath,
                                       @Value("${conductor.tasks.google-pubsub-publish.projectId:10}") String projectId,
                                       @Value("${conductor.tasks.google-pubsub-publish.cacheSize:10}") int cacheSize,
                                       @Value("${conductor.tasks.google-pubsub-publish:120000ms}") Duration cacheTime) {
        this.projectId=projectId;
        try {
            if (googleApplicationCredentialsPath!=null && !googleApplicationCredentialsPath.equals("")){
                LOGGER.debug("Found googleApplicationCredentialsPath");
                googleCredentialsProvider =
                        FixedCredentialsProvider.create(
                                ServiceAccountCredentials.fromStream(new FileInputStream(googleApplicationCredentialsPath)));
            }
        } catch (IOException e) {
            String msg = "Could not load googleApplicationCredentialsPath";
            LOGGER.error(msg);
            throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, msg);
        }
        this.googlePublisherProducerCache = CacheBuilder.newBuilder().removalListener(LISTENER)
            .maximumSize(cacheSize).expireAfterAccess(cacheTime.toMillis(), TimeUnit.MILLISECONDS)
            .build();
    }

    public Publisher getPublisher(GooglePublishTask.Input input) {
        return getFromCache(input, () -> {
            TopicName topicName = TopicName.of(projectId, input.getTopicId());
            if (googleCredentialsProvider!=null){
                return Publisher.newBuilder(topicName)
                        .setCredentialsProvider(googleCredentialsProvider).build();
            }else {
                return Publisher.newBuilder(topicName).build();
            }
        });
    }

    @VisibleForTesting
    Publisher getFromCache(GooglePublishTask.Input input, Callable<Publisher> createProducerCallable) {
        try {
            return googlePublisherProducerCache.get(input, createProducerCallable);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

}
