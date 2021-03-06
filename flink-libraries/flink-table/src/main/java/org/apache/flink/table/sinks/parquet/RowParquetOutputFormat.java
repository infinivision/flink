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

package org.apache.flink.table.sinks.parquet;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.SafetyNetWrapperFileSystem;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.table.api.types.IntType;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.StringType;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.sources.parquet.ParquetSchemaConverter;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A subclass of {@link OutputFormat} to write {@link BaseRow} to Parquet files.
 */
public class RowParquetOutputFormat implements OutputFormat<BaseRow> {

	public static final Logger LOG = LoggerFactory.getLogger(RowParquetOutputFormat.class);
	private static final int DEFAULT_BLOCK_SIZE = 128 * 1024 * 1024;
	private static final String FILE_PREFIX_NAME = "parquet-";

	private final InternalType[] fieldTypes;
	private final String[] fieldNames;
	private int[] partitionColumnsIndex;
	private final String dir;
	private int blockSize;
	private boolean enableDictionary;
	private CompressionCodecName compression;
	private String filePrefixName;
	private int taskNumber;
	private int numTasks;
	private JobConf jobConf;
	private String lastPartition = "";

	private org.apache.hadoop.mapreduce.RecordWriter<Void, BaseRow> realWriter;
	private ConcurrentHashMap<Object, org.apache.hadoop.mapreduce.RecordWriter<Void, BaseRow>> writers = new ConcurrentHashMap<>();
	private TaskAttemptContext taskContext;

	public RowParquetOutputFormat(
		String dir, InternalType[] fieldTypes, String[] fieldNames, CompressionCodecName compression) {
		this(dir, fieldTypes, fieldNames, compression, DEFAULT_BLOCK_SIZE, false);
	}

	public RowParquetOutputFormat(
		String dir, InternalType[] fieldTypes, String[] fieldNames) {
		this(dir, fieldTypes, fieldNames, CompressionCodecName.UNCOMPRESSED, DEFAULT_BLOCK_SIZE, false);
	}

	public RowParquetOutputFormat(
		String dir, InternalType[] fieldTypes, String[] fieldNames,
		CompressionCodecName compression, int blockSize, boolean enableDictionary) {
		this(dir, fieldTypes, fieldNames, compression, blockSize, enableDictionary, FILE_PREFIX_NAME);
	}

	public RowParquetOutputFormat(
		String dir, InternalType[] fieldTypes, String[] fieldNames,
		CompressionCodecName compression, int blockSize, boolean enableDictionary, String[] partitionColumns) {
		this(dir, fieldTypes, fieldNames, compression, blockSize, enableDictionary, FILE_PREFIX_NAME);
		this.partitionColumnsIndex = new int[partitionColumns.length];
		List<String> fieldList = Arrays.asList(fieldNames);
		for (int i = 0; i < partitionColumns.length; i++) {
			this.partitionColumnsIndex[i] = fieldList.indexOf(partitionColumns[i]);
		}
	}

	public RowParquetOutputFormat(
		String dir, InternalType[] fieldTypes, String[] fieldNames,
		CompressionCodecName compression, int blockSize, boolean enableDictionary, String filePrefixName) {
		Preconditions.checkArgument(fieldNames != null && fieldNames.length > 0);
		Preconditions.checkArgument(fieldTypes != null && fieldTypes.length == fieldNames.length);
		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;
		this.dir = dir;
		this.blockSize = blockSize;
		this.enableDictionary = enableDictionary;
		this.compression = compression;
		this.filePrefixName = filePrefixName;
	}

	@Override
	public void configure(Configuration parameters) {
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		this.taskNumber = taskNumber;
		this.numTasks = numTasks;
		jobConf = new JobConf();
		// create a TaskInputOutputContext

		TaskAttemptID taskAttemptID = new TaskAttemptID();
		RowWritableWriteSupport.setSchema(ParquetSchemaConverter.convert(fieldNames, fieldTypes), jobConf);

		taskContext = ContextUtil.newTaskAttemptContext(jobConf, taskAttemptID);
		LOG.info("initialize serde with table properties.");
		initializeSerProperties(taskContext);
		if (partitionColumnsIndex.length == 0) { // single partition
			createWriter(taskNumber, numTasks, null);
		}
	}

	private RecordWriter<Void, BaseRow> createWriter(int taskNumber, int numTasks, String partitionDir) throws IOException {

		// init and register file system
		String fileName = StringUtils.isBlank(partitionDir) ? "" : partitionDir;
		fileName += filePrefixName + numTasks + "-" +
			taskNumber + ".parquet";
		Path path = new Path(dir, fileName);
		FileSystem fs = path.getFileSystem();
		if (fs instanceof SafetyNetWrapperFileSystem) {
			fs = ((SafetyNetWrapperFileSystem) fs).getWrappedDelegate();
		}

		if (fs instanceof HadoopFileSystem) {
			jobConf.addResource(((HadoopFileSystem) fs).getConfig());
		}

		if (!(fs instanceof LocalFileSystem || fs instanceof HadoopFileSystem)) {
			throw new RuntimeException("FileSystem: " + fs.getClass().getCanonicalName() + " is not supported.");
		}

		// clean up output file in case of failover.
		fs.delete(path, true);

		ParquetOutputFormat realOutputFormat = new ParquetOutputFormat(new RowWritableWriteSupport(fieldTypes));

		LOG.info("creating new record writer..." + this);

		try {
			LOG.info("creating real writer to write at " + dir + fileName);

			realWriter = realOutputFormat.getRecordWriter(
				taskContext, new org.apache.hadoop.fs.Path(path.toUri()));
			if (StringUtils.isNoneBlank(partitionDir)) {
				this.writers.put(partitionDir, realWriter);
			}
			LOG.info("real writer: " + realWriter);
			return realWriter;
		} catch (final InterruptedException e) {
			throw new IOException(e);
		}
	}

	private void initializeSerProperties(JobContext job) {
		org.apache.hadoop.conf.Configuration conf = ContextUtil.getConfiguration(job);
		if (blockSize > 0) {
			LOG.info("get override parquet.block.size property with: {}", blockSize);
			conf.setInt(ParquetOutputFormat.BLOCK_SIZE, blockSize);

			LOG.info("get override dfs.blocksize property with: {}", blockSize);
			conf.setInt("dfs.blocksize", blockSize);
		}

		LOG.info("get override parquet.enable.dictionary property with: {}", enableDictionary);
		conf.setBoolean(
			ParquetOutputFormat.ENABLE_DICTIONARY, enableDictionary);

		if (compression != null) {
			//get override compression properties via "tblproperties" clause if it is set
			LOG.info("get override compression properties with {}", compression.name());
			conf.set(ParquetOutputFormat.COMPRESSION, compression.name());
		}
	}

	@Override
	public void writeRecord(BaseRow record) throws IOException {
		try {
				if (partitionColumnsIndex != null && partitionColumnsIndex.length > 0) {
					createWriterIfNeed(record).write(null, record);
				} else {
					realWriter.write(null, record);
				}
		} catch (final InterruptedException e) {
			throw new IOException(e);
		}
	}

	private String getPartitionDir(BaseRow record) {
		StringBuffer dir = new StringBuffer();
		for (int i : partitionColumnsIndex) {
			dir.append(fieldNames[i]);
			dir.append('=');
			InternalType fieldType = fieldTypes[i];
			String value;
			if (fieldType instanceof StringType) {
				value = record.getString(i);
			} else if (fieldType instanceof IntType) {
				value = String.valueOf(record.getInt(i));
			} else {
				throw new RuntimeException("unsupported partition column type" + fieldType.toString());
			}
			dir.append(value);
			dir.append('/');
		}
		return dir.toString();
	}

	private RecordWriter<Void, BaseRow> createWriterIfNeed(BaseRow record) throws IOException {
		String dir = getPartitionDir(record);
		if (!this.lastPartition.equals(dir)) {
			close(writers.get(this.lastPartition));
			writers.remove(this.lastPartition);
			lastPartition = dir;
		}
		if (!writers.containsKey(dir)) {
			writers.put(dir, createWriter(taskNumber, numTasks, dir));
		}
		return writers.get(dir);
	}

	private void close(RecordWriter<Void, BaseRow> writer) throws IOException {
		if (writer != null) {
			try {
				writer.close(taskContext);
			} catch (InterruptedException e) {
				throw  new IOException(e);
			}
		}
	}

	@Override
	public void close() throws IOException {
		if (writers.size() > 0) {
			for (RecordWriter<Void, BaseRow> writer : writers.values()) {
				close(writer);
			}
		} else {
			close(realWriter);
		}
	}
}
