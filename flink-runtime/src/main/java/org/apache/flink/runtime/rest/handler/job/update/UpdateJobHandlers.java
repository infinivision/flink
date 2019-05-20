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
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.async.AbstractAsynchronousOperationHandlers;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationInfo;
import org.apache.flink.runtime.rest.handler.job.AsynchronousJobOperationKey;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdQueryParameter;
import org.apache.flink.runtime.rest.messages.RescalingParallelismQueryParameter;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.update.JobUpdateRequest;
import org.apache.flink.runtime.update.action.JobUpdateAction;
import org.apache.flink.runtime.update.action.JobVertexRescaleAction;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.SerializedThrowable;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Rest handler to trigger and poll the rescaling of a running job's vertex.
 */
public class UpdateJobHandlers extends AbstractAsynchronousOperationHandlers<AsynchronousJobOperationKey, Acknowledge> {

	/**
	 * trigger.
	 */
	public class UpdateJobTriggerHandler extends TriggerHandler<RestfulGateway, EmptyRequestBody, UpdateJobTriggerMessageParameters> {

		public UpdateJobTriggerHandler(CompletableFuture<String> localRestAddress, GatewayRetriever<? extends RestfulGateway> leaderRetriever, Time timeout, Map<String, String> responseHeaders) {
			super(localRestAddress, leaderRetriever, timeout, responseHeaders, UpdateJobTriggerHeaders.getInstance());
		}

		@Override
		protected CompletableFuture<Acknowledge> triggerOperation(HandlerRequest<EmptyRequestBody, UpdateJobTriggerMessageParameters> request, RestfulGateway gateway) throws RestHandlerException {
			final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
			final List<JobVertexID> vertexIds = request.getQueryParameter(JobVertexIdQueryParameter.class);
			final List<Integer> parallelism = request.getQueryParameter(RescalingParallelismQueryParameter.class);

			if (vertexIds.isEmpty() || parallelism.isEmpty()) {
				throw new RestHandlerException("No new parallelism was specified.", HttpResponseStatus.BAD_REQUEST);
			}
			if (vertexIds.size() != parallelism.size()) {
				throw new RestHandlerException("vertexs length != parallelisms.length.", HttpResponseStatus.BAD_REQUEST);
			}

			List<JobUpdateAction> actions = new ArrayList<>();
			int parallelismIndex = 0;
			for (JobVertexID vertexId : vertexIds) {
				actions.add(new JobVertexRescaleAction(vertexId, parallelism.get(parallelismIndex++)));
			}
			JobUpdateRequest req = new JobUpdateRequest(actions);

			final CompletableFuture<Acknowledge> future = gateway.updateJob(jobId, req, RpcUtils.INF_TIMEOUT);

			return future;
		}

		@Override
		protected AsynchronousJobOperationKey createOperationKey(HandlerRequest<EmptyRequestBody, UpdateJobTriggerMessageParameters> request) {
			final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
			return AsynchronousJobOperationKey.of(new TriggerId(), jobId);
		}
	}

	/**
	 * status.
	 */
	public class UpdateJobStatusHandler extends StatusHandler<RestfulGateway, AsynchronousOperationInfo, UpdateJobStatusMessageParameters>{

		public UpdateJobStatusHandler(CompletableFuture<String> localRestAddress, GatewayRetriever<? extends RestfulGateway> leaderRetriever, Time timeout, Map<String, String> responseHeaders) {
			super(localRestAddress, leaderRetriever, timeout, responseHeaders, UpdateJobStatusHeaders.getInstance());
		}

		/**
		 * Extract the operation key under which the operation result future is stored.
		 *
		 * @param request with which the status handler has been called
		 * @return Operation key under which the operation result future is stored
		 */
		@Override
		protected AsynchronousJobOperationKey getOperationKey(HandlerRequest<EmptyRequestBody, UpdateJobStatusMessageParameters> request) {
			final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
			final TriggerId triggerId = request.getPathParameter(TriggerIdPathParameter.class);

			return AsynchronousJobOperationKey.of(triggerId, jobId);
		}

		/**
		 * Create an exceptional operation result from the given {@link Throwable}. This
		 * method is called if the asynchronous operation failed.
		 *
		 * @param throwable failure of the asynchronous operation
		 * @return Exceptional operation result
		 */
		@Override
		protected AsynchronousOperationInfo exceptionalOperationResultResponse(Throwable throwable) {
			return AsynchronousOperationInfo.completeExceptional(new SerializedThrowable(throwable));
		}

		/**
		 * Create the operation result from the given value.
		 *
		 * @param operationResult of the asynchronous operation
		 * @return Operation result
		 */
		@Override
		protected AsynchronousOperationInfo operationResultResponse(Acknowledge operationResult) {
			return AsynchronousOperationInfo.complete();
		}
	}

}
