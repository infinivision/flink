/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.streaming.connectors.kafka.v2;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaMetricWrapper;
import org.apache.flink.streaming.connectors.kafka.v2.common.Syncable;
import org.apache.flink.streaming.connectors.kafka.v2.common.TupleRichOutputFormat;
import org.apache.flink.types.Row;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.SerializableObject;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

/** Kafka OutputFormat base class. */
public abstract class KafkaBaseOutputFormat extends TupleRichOutputFormat implements Syncable {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaBaseOutputFormat.class);

	private static final long serialVersionUID = 1L;

	/**
	 * Configuration key for disabling the metrics reporting.
	 */
	public static final String KEY_DISABLE_METRICS = "flink.disable-metrics";

	/**
	 * User defined properties for the Producer.
	 */
	protected final Properties producerConfig;

	/**
	 * The name of the default topic this producer is writing data to.
	 */
	protected final String defaultTopicId;

	/**
	 * (Serializable) SerializationSchema for turning objects used with Flink into.
	 * byte[] for Kafka.
	 */
	protected final KafkaConverter<Row> schema;

	/**
	 * Partitions of each topic.
	 */
	protected final Map<String, int[]> topicPartitionsMap;

	/**
	 * Flag indicating whether to accept failures (and log them), or to fail on failures.
	 */
	protected boolean logFailuresOnly;

	/**
	 * If true, the producer will wait until all outstanding records have been send to the broker.
	 */
	protected boolean flushOnCheckpoint;

	// -------------------------------- Runtime fields ------------------------------------------

	/** KafkaProducer instance. */
	protected transient KafkaProducer<byte[], byte[]> producer;

	/** The callback than handles error propagation or logging callbacks. */
	protected transient Callback callback;

	/** Errors encountered in the async producer are stored here. */
	protected transient volatile Exception asyncException;

	/** Lock for accessing the pending records. */
	protected final SerializableObject pendingRecordsLock = new SerializableObject();

	/** Number of unacknowledged records. */
	protected long pendingRecords;

	public KafkaBaseOutputFormat(
			String defaultTopicId, KafkaConverter serializationSchema, Properties
			producerConfig) {
		requireNonNull(defaultTopicId, "TopicID not set");
		requireNonNull(serializationSchema, "serializationSchema not set");
		requireNonNull(producerConfig, "producerConfig not set");
		ClosureCleaner.ensureSerializable(serializationSchema);

		this.defaultTopicId = defaultTopicId;
		this.schema = serializationSchema;
		this.producerConfig = producerConfig;

		// set the producer configuration properties for kafka record key value serializers.
		if (!producerConfig.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
			this.producerConfig.put(
					ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
					ByteArraySerializer.class.getCanonicalName());
		} else {
			LOG.warn("Overwriting the '{}' is not recommended", ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
		}

		if (!producerConfig.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)) {
			this.producerConfig.put(
					ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
					ByteArraySerializer.class.getCanonicalName());
		} else {
			LOG.warn("Overwriting the '{}' is not recommended", ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
		}

		// eagerly ensure that bootstrap servers are set.
		if (!this.producerConfig.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
			throw new IllegalArgumentException(
					ProducerConfig.BOOTSTRAP_SERVERS_CONFIG + " must be supplied in the producer config properties.");
		}

		this.topicPartitionsMap = new HashMap<>();
	}

	// ---------------------------------- Properties --------------------------

	/**
	 * Defines whether the producer should fail on errors, or only log them.
	 * If this is set to true, then exceptions will be only logged, if set to false,
	 * exceptions will be eventually thrown and cause the streaming program to
	 * fail (and enter recovery).
	 *
	 * @param logFailuresOnly The flag to indicate logging-only on exceptions.
	 */
	public void setLogFailuresOnly(boolean logFailuresOnly) {
		this.logFailuresOnly = logFailuresOnly;
	}

	/**
	 * If set to true, the Flink producer will wait for all outstanding messages in the Kafka buffers
	 * to be acknowledged by the Kafka producer on a checkpoint.
	 * This way, the producer can guarantee that messages in the Kafka buffers are part of the checkpoint.
	 *
	 * @param flush Flag indicating the flushing mode (true = flush on checkpoint)
	 */
	public void setFlushOnCheckpoint(boolean flush) {
		this.flushOnCheckpoint = flush;
	}

	/**
	 * Used for testing only.
	 */
	@VisibleForTesting
	protected <K, V> KafkaProducer<K, V> getKafkaProducer(Properties props) {
		return createKafkaProducer(props);
	}


	// load KafkaProducer via system classloader instead of application classloader, otherwise we will hit classloading
	// issue in scala-shell scenario where kafka jar is not shipped with JobGraph, but shipped when starting yarn
	// container.
	private static KafkaProducer createKafkaProducer(Properties kafkaProperties) {
		try {
			return new KafkaProducer<>(kafkaProperties);
		} catch (KafkaException e) {
			ClassLoader original = Thread.currentThread().getContextClassLoader();
			try {
				Thread.currentThread().setContextClassLoader(null);
				return new KafkaProducer<>(kafkaProperties);
			} finally {
				Thread.currentThread().setContextClassLoader(original);
			}
		}
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		super.open(taskNumber, numTasks);
		producer = getKafkaProducer(this.producerConfig);

		RuntimeContext ctx = getRuntimeContext();
		schema.open(ctx);

		LOG.info("Starting FlinkKafkaProducer ({}/{}) to produce into default topic {}",
				ctx.getIndexOfThisSubtask() + 1, ctx.getNumberOfParallelSubtasks(), defaultTopicId);

		// register Kafka metrics to Flink accumulators
		if (!Boolean.parseBoolean(producerConfig.getProperty(KEY_DISABLE_METRICS, "false"))) {
			Map<MetricName, ? extends Metric> metrics = this.producer.metrics();

			if (metrics == null) {
				// MapR's Kafka implementation returns null here.
				LOG.info("Producer implementation does not support metrics");
			} else {
				final MetricGroup kafkaMetricGroup = getRuntimeContext().getMetricGroup().addGroup("KafkaProducer");
				for (Map.Entry<MetricName, ? extends Metric> metric : metrics.entrySet()) {
					kafkaMetricGroup.gauge(metric.getKey().name(), new KafkaMetricWrapper(metric.getValue()));
				}
			}
		}

		if (logFailuresOnly) {
			callback = new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception e) {
					if (e != null) {
						LOG.error("Error while sending record to Kafka: " + e.getMessage(), e);
					}
					acknowledgeMessage();
				}
			};
		} else {
			callback = new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if (exception != null && asyncException == null) {
						asyncException = exception;
					}
					acknowledgeMessage();
				}
			};
		}
	}

	@Override
	public void close() throws IOException {
		schema.close();
	}

	// ------------------- Logic for handling checkpoint flushing -------------------------- //

	private void acknowledgeMessage() {
		if (flushOnCheckpoint) {
			synchronized (pendingRecordsLock) {
				pendingRecords--;
				if (pendingRecords == 0) {
					pendingRecordsLock.notifyAll();
				}
			}
		}
	}

	protected abstract void flush();

	@Override
	public void writeAddRecord(Row row) throws IOException {
		// propagate asynchronous errors
		checkErroneous();

		String targetTopic = schema.getTargetTopic(row);
		if (targetTopic == null) {
			targetTopic = defaultTopicId;
		}

		int[] partitions = this.topicPartitionsMap.get(targetTopic);
		if (null == partitions) {
			partitions = getPartitionsByTopic(targetTopic, producer);
			this.topicPartitionsMap.put(targetTopic, partitions);
		}

		ProducerRecord<byte[], byte[]> record;
		record = schema.convert(row, targetTopic, partitions);
		if (flushOnCheckpoint) {
			synchronized (pendingRecordsLock) {
				pendingRecords++;
			}
		}
		if (record != null) {
			producer.send(record, callback);
		}
	}

	@Override
	public void writeDeleteRecord(Row row) throws IOException {

	}

	@Override
	public void sync() throws IOException {
		// check for asynchronous errors and fail the checkpoint if necessary
		checkErroneous();

		if (flushOnCheckpoint) {
			// flushing is activated: We need to wait until pendingRecords is 0
			flush();
			synchronized (pendingRecordsLock) {
				if (pendingRecords != 0) {
					throw new IllegalStateException(
						"Pending record count must be zero at this point: " + pendingRecords);
				}

				// if the flushed requests has errors, we should propagate it also and fail the checkpoint
				checkErroneous();
			}
		}
	}

	@Override
	public String getName() {
		return null;
	}

	@Override
	public void configure(Configuration parameters) {

	}
	// ----------------------------------- Utilities --------------------------

	protected void checkErroneous() {
		Exception e = asyncException;
		if (e != null) {
			// prevent double throwing
			asyncException = null;
			throw new RuntimeException("Failed to send data to Kafka: " + e.getMessage(), e);
		}
	}

	public static Properties getPropertiesFromBrokerList(String brokerList) {
		String[] elements = brokerList.split(",");

		// validate the broker addresses
		for (String broker : elements) {
			NetUtils.getCorrectHostnamePort(broker);
		}

		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
		return props;
	}

	protected static int[] getPartitionsByTopic(String topic, KafkaProducer<byte[], byte[]> producer) {
		// the fetched list is immutable, so we're creating a mutable copy in order to sort it
		List<PartitionInfo> partitionsList = new ArrayList<>(producer.partitionsFor(topic));

		// sort the partitions by partition id to make sure the fetched partition list is the same across subtasks
		Collections.sort(partitionsList, new Comparator<PartitionInfo>() {
			@Override
			public int compare(PartitionInfo o1, PartitionInfo o2) {
				return Integer.compare(o1.partition(), o2.partition());
			}
		});

		int[] partitions = new int[partitionsList.size()];
		for (int i = 0; i < partitions.length; i++) {
			partitions[i] = partitionsList.get(i).partition();
		}

		return partitions;
	}

	@VisibleForTesting
	protected long numPendingRecords() {
		synchronized (pendingRecordsLock) {
			return pendingRecords;
		}
	}
}
