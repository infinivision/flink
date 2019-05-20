/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler.job.update;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationResult;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdQueryParameter;
import org.apache.flink.runtime.rest.messages.RescalingParallelismQueryParameter;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;


/**
 * Test.
 */
public class UpdateJobTest extends TestLogger {

	private static final CompletableFuture<String> LOCAL_REST_ADDRESS =
		CompletableFuture.completedFuture("localhost:12345");

	private static final Time TIMEOUT = Time.seconds(10);

	private static final JobID JOB_ID = new JobID();

	private UpdateJobHandlers.UpdateJobTriggerHandler updateJobTriggerHandler;

	private UpdateJobHandlers.UpdateJobStatusHandler updateJobStatusHandler;

	private GatewayRetriever<RestfulGateway> leaderRetriever;

	@Before
	public void setUp() throws Exception {
		leaderRetriever = () -> CompletableFuture.completedFuture(null);

		final UpdateJobHandlers updateJobHandlers = new UpdateJobHandlers();
		updateJobTriggerHandler = updateJobHandlers.new UpdateJobTriggerHandler(
			LOCAL_REST_ADDRESS,
			leaderRetriever,
			TIMEOUT,
			Collections.emptyMap());

		updateJobStatusHandler = updateJobHandlers.new UpdateJobStatusHandler(
			LOCAL_REST_ADDRESS,
			leaderRetriever,
			TIMEOUT,
			Collections.emptyMap());
	}

	@Test
	public void testUpdateJobCompletedSuccessfully() throws Exception {
		final TestingRestfulGateway testingRestfulGateway = new TestingRestfulGateway.Builder()
			.setUpdateJobFunction((JobID jobId) -> CompletableFuture.completedFuture(Acknowledge.get()))
			.build();

		Map<String, String> patchs = new HashMap<>();
		patchs.put(JobIDPathParameter.KEY, JOB_ID.toString());

		Map<String, List<String>> parameters = new HashMap<>();
		List<String> parallelism = new ArrayList<>();
		parallelism.add("1");
		parallelism.add("2");
		parameters.put(RescalingParallelismQueryParameter.KEY, parallelism);
		List<String> vertexId = new ArrayList<>();
		vertexId.add("cbc357ccb763df2852fee8c4fc7d55f2");
		parameters.put(JobVertexIdQueryParameter.KEY, vertexId);

		HandlerRequest<EmptyRequestBody, UpdateJobTriggerMessageParameters> request = new HandlerRequest<>(
			EmptyRequestBody.getInstance(),
			new UpdateJobTriggerMessageParameters(),
			patchs,
			parameters);

		final TriggerId triggerId = updateJobTriggerHandler.handleRequest(
			request,
			testingRestfulGateway).get().getTriggerId();

		Map<String, String> patchsStatus = new HashMap<>();
		patchsStatus.put(JobIDPathParameter.KEY, JOB_ID.toString());
		patchsStatus.put(TriggerIdPathParameter.KEY, triggerId.toString());

		HandlerRequest<EmptyRequestBody, UpdateJobStatusMessageParameters> requestStatus = new HandlerRequest<>(
			EmptyRequestBody.getInstance(),
			new UpdateJobStatusMessageParameters(),
			patchsStatus,
			Collections.emptyMap());
		AsynchronousOperationResult<Acknowledge> responseBody;
			updateJobStatusHandler.handleRequest(
			requestStatus,
			testingRestfulGateway).get();
	}
}
