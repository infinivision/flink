/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file exceBinaryRow in compliance
 * with the License.  You may oBinaryRowain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHBinaryRow WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.join.batch;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.streaming.api.operators.TwoInputSelection;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.types.RowType;
import org.apache.flink.table.codegen.CodeGenUtils;
import org.apache.flink.table.codegen.GeneratedJoinConditionFunction;
import org.apache.flink.table.codegen.GeneratedProjection;
import org.apache.flink.table.codegen.JoinConditionFunction;
import org.apache.flink.table.codegen.Projection;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.runtime.AbstractStreamOperatorWithMetrics;
import org.apache.flink.table.runtime.join.batch.hashtable.BinaryHashTable;
import org.apache.flink.table.runtime.util.RowIterator;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.table.typeutils.AbstractRowSerializer;
import org.apache.flink.util.Collector;

import org.codehaus.commons.compiler.CompileException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Shuffle hash join base operator.
 * The join operator implements the logic of a join operator at runtime. It uses a
 * hybrid-hash-join internally to match the records with equal key. The build side
 * of the hash is the first input of the match.
 */
public abstract class HashJoinOperator extends AbstractStreamOperatorWithMetrics<BaseRow>
		implements TwoInputStreamOperator<BaseRow, BaseRow, BaseRow> {

	private static final Logger LOG = LoggerFactory.getLogger(HashJoinOperator.class);

	private final HashJoinParameter parameter;
	private final boolean reverseJoinFunction;
	final HashJoinType type;

	// cooked classes
	transient Class<JoinConditionFunction> condFuncClass;
	transient Class<Projection<BaseRow, BinaryRow>> buildProjectionClass;
	transient Class<Projection<BaseRow, BinaryRow>> probeProjectionClass;

	private transient BinaryHashTable table;
	transient Collector<BaseRow> collector;

	transient BaseRow buildSideNullRow;
	transient BaseRow probeSideNullRow;
	private transient JoinedRow joinedRow;

	HashJoinOperator(HashJoinParameter parameter) {
		this.parameter = parameter;
		this.type = parameter.type;
		this.reverseJoinFunction = parameter.reverseJoinFunction;
	}

	@Override
	public void open() throws Exception {
		super.open();

		// code gen function and projection classes.
		cookGeneratedClasses(getContainingTask().getUserCodeClassLoader());

		IOManager ioManager = getContainingTask().getEnvironment().getIOManager();

		final AbstractRowSerializer buildSerializer = (AbstractRowSerializer) getOperatorConfig()
			.getTypeSerializerIn1(getUserCodeClassloader());
		final AbstractRowSerializer probeSerializer = (AbstractRowSerializer) getOperatorConfig()
			.getTypeSerializerIn2(getUserCodeClassloader());

		boolean hashJoinUseBitMaps =
				getContainingTask().getEnvironment().getTaskConfiguration().getBoolean(
						ConfigConstants.RUNTIME_HASH_JOIN_BLOOM_FILTERS_KEY,
						ConfigConstants.DEFAULT_RUNTIME_HASH_JOIN_BLOOM_FILTERS);

		int parallel = getRuntimeContext().getNumberOfParallelSubtasks();

		this.table = new BinaryHashTable(
				getSqlConf(),
				getContainingTask(),
				buildSerializer, probeSerializer,
				buildProjectionClass.newInstance(), probeProjectionClass.newInstance(),
				getContainingTask().getEnvironment().getMemoryManager(),
				parameter.reservedMemorySize,
				parameter.maxMemorySize,
				parameter.perRequestMemorySize,
				ioManager, parameter.buildRowSize, parameter.buildRowCount / parallel,
				hashJoinUseBitMaps, type,
				condFuncClass.newInstance(), reverseJoinFunction, parameter.filterNullKeys,
				parameter.tryDistinctBuildRow);

		this.collector = new StreamRecordCollector<>(output);

		this.buildSideNullRow = new GenericRow(buildSerializer.getNumFields());
		this.probeSideNullRow = new GenericRow(probeSerializer.getNumFields());
		this.joinedRow = new JoinedRow();

		getMetricGroup().gauge("memoryUsedSizeInBytes", table::getUsedMemoryInBytes);
		getMetricGroup().gauge("numSpillFiles", table::getNumSpillFiles);
		getMetricGroup().gauge("spillInBytes", table::getSpillInBytes);
	}

	protected void cookGeneratedClasses(ClassLoader cl) throws CompileException {
		long startTime = System.currentTimeMillis();
		condFuncClass = CodeGenUtils.compile(
				cl, parameter.condFuncCode.name(), parameter.condFuncCode.code());
		buildProjectionClass = CodeGenUtils.compile(
				cl, parameter.buildProjectionCode.name(), parameter.buildProjectionCode.code());
		probeProjectionClass = CodeGenUtils.compile(
				cl, parameter.probeProjectionCode.name(), parameter.probeProjectionCode.code());
		parameter.condFuncCode = null;
		parameter.buildProjectionCode = null;
		parameter.probeProjectionCode = null;
		long endTime = System.currentTimeMillis();
		LOG.info("Compiling generated codes, used time: " + (endTime - startTime) + "ms.");
	}

	@Override
	public TwoInputSelection firstInputSelection() {
		return TwoInputSelection.FIRST;
	}

	@Override
	public TwoInputSelection processElement1(StreamRecord<BaseRow> element) throws Exception {
		this.table.putBuildRow(element.getValue());
		return TwoInputSelection.FIRST;
	}

	@Override
	public TwoInputSelection processElement2(StreamRecord<BaseRow> element) throws Exception {
		if (this.table.tryProbe(element.getValue())) {
			joinWithNextKey();
		}
		return TwoInputSelection.SECOND;
	}

	@Override
	public void endInput1() throws Exception {
		LOG.info("Finish build phase.");
		this.table.endBuild();
	}

	@Override
	public void endInput2() throws Exception {
		LOG.info("Finish probe phase.");
		while (this.table.nextMatching()) {
			joinWithNextKey();
		}
		LOG.info("Finish rebuild phase.");
	}

	private void joinWithNextKey() throws Exception {
		// we have a next record, get the iterators to the probe and build side values
		join(table.getBuildSideIterator(), table.getCurrentProbeRow());
	}

	public abstract void join(RowIterator<BinaryRow> buildIter, BaseRow probeRow) throws Exception;

	void innerJoin(RowIterator<BinaryRow> buildIter, BaseRow probeRow) throws Exception {
		collect(buildIter.getRow(), probeRow);
		while (buildIter.advanceNext()) {
			collect(buildIter.getRow(), probeRow);
		}
	}

	void buildOuterJoin(RowIterator<BinaryRow> buildIter) throws Exception {
		collect(buildIter.getRow(), probeSideNullRow);
		while (buildIter.advanceNext()) {
			collect(buildIter.getRow(), probeSideNullRow);
		}
	}

	void collect(BaseRow row1, BaseRow row2) throws Exception {
		if (reverseJoinFunction) {
			collector.collect(joinedRow.replace(row2, row1));
		} else {
			collector.collect(joinedRow.replace(row1, row2));
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
		if (this.table != null) {
			this.table.close();
			this.table.free();
			this.table = null;
		}
	}

	public static HashJoinOperator newHashJoinOperator(
			long minMemorySize,
			long maxMemorySize,
			long eachRequestMemorySize,
			HashJoinType type,
			GeneratedJoinConditionFunction condFuncCode,
			boolean reverseJoinFunction,
			boolean[] filterNullKeys,
			GeneratedProjection buildProjectionCode,
			GeneratedProjection probeProjectionCode,
			boolean tryDistinctBuildRow,
			int buildRowSize,
			long buildRowCount,
			long probeRowCount,
			RowType keyType) {
		HashJoinParameter parameter = new HashJoinParameter(minMemorySize, maxMemorySize, eachRequestMemorySize,
				type, condFuncCode, reverseJoinFunction, filterNullKeys, buildProjectionCode, probeProjectionCode,
				tryDistinctBuildRow, buildRowSize, buildRowCount, probeRowCount, keyType);
		switch (type) {
			case INNER:
				return new InnerHashJoinOperator(parameter);
			case BUILD_OUTER:
				return new BuildOuterHashJoinOperator(parameter);
			case PROBE_OUTER:
				return new ProbeOuterHashJoinOperator(parameter);
			case FULL_OUTER:
				return new FullOuterHashJoinOperator(parameter);
			case SEMI:
				return new SemiHashJoinOperator(parameter);
			case ANTI:
				return new AntiHashJoinOperator(parameter);
			case BUILD_LEFT_SEMI:
			case BUILD_LEFT_ANTI:
				return new BuildLeftSemiOrAntiHashJoinOperator(parameter);
			default:
				throw new IllegalArgumentException("invalid: " + type);
		}
	}

	static class HashJoinParameter implements Serializable {
		long reservedMemorySize;
		long maxMemorySize;
		long perRequestMemorySize;
		HashJoinType type;
		GeneratedJoinConditionFunction condFuncCode;
		boolean reverseJoinFunction;
		boolean[] filterNullKeys;
		GeneratedProjection buildProjectionCode;
		GeneratedProjection probeProjectionCode;
		boolean tryDistinctBuildRow;
		int buildRowSize;
		long buildRowCount;
		long probeRowCount;
		RowType keyType;

		HashJoinParameter(
				long reservedMemorySize, long maxMemorySize, long perRequestMemorySize, HashJoinType type,
				GeneratedJoinConditionFunction condFuncCode, boolean reverseJoinFunction,
				boolean[] filterNullKeys,
				GeneratedProjection buildProjectionCode,
				GeneratedProjection probeProjectionCode, boolean tryDistinctBuildRow,
				int buildRowSize, long buildRowCount, long probeRowCount, RowType keyType) {
			this.reservedMemorySize = reservedMemorySize;
			this.maxMemorySize = maxMemorySize;
			this.perRequestMemorySize = perRequestMemorySize;
			this.type = type;
			this.condFuncCode = condFuncCode;
			this.reverseJoinFunction = reverseJoinFunction;
			this.filterNullKeys = filterNullKeys;
			this.buildProjectionCode = buildProjectionCode;
			this.probeProjectionCode = probeProjectionCode;
			this.tryDistinctBuildRow = tryDistinctBuildRow;
			this.buildRowSize = buildRowSize;
			this.buildRowCount = buildRowCount;
			this.probeRowCount = probeRowCount;
			this.keyType = keyType;
		}
	}

	/**
	 * Inner join.
	 */
	private static class InnerHashJoinOperator extends HashJoinOperator {

		InnerHashJoinOperator(HashJoinParameter parameter) {
			super(parameter);
		}

		@Override
		public void join(RowIterator<BinaryRow> buildIter, BaseRow probeRow) throws Exception {
			if (buildIter.advanceNext()) {
				if (probeRow != null) {
					innerJoin(buildIter, probeRow);
				}
			}
		}
	}

	/**
	 * BuildOuter join.
	 */
	private static class BuildOuterHashJoinOperator extends HashJoinOperator {

		BuildOuterHashJoinOperator(HashJoinParameter parameter) {
			super(parameter);
		}

		@Override
		public void join(RowIterator<BinaryRow> buildIter, BaseRow probeRow) throws Exception {
			if (buildIter.advanceNext()) {
				if (probeRow != null) {
					innerJoin(buildIter, probeRow);
				} else {
					buildOuterJoin(buildIter);
				}
			}
		}
	}

	/**
	 * ProbeOuter join.
	 */
	private static class ProbeOuterHashJoinOperator extends HashJoinOperator {

		ProbeOuterHashJoinOperator(HashJoinParameter parameter) {
			super(parameter);
		}

		@Override
		public void join(RowIterator<BinaryRow> buildIter, BaseRow probeRow) throws Exception {
			if (buildIter.advanceNext()) {
				if (probeRow != null) {
					innerJoin(buildIter, probeRow);
				}
			} else if (probeRow != null) {
				collect(buildSideNullRow, probeRow);
			}
		}
	}

	/**
	 * FullOuter join.
	 */
	private static class FullOuterHashJoinOperator extends HashJoinOperator {

		FullOuterHashJoinOperator(HashJoinParameter parameter) {
			super(parameter);
		}

		@Override
		public void join(RowIterator<BinaryRow> buildIter, BaseRow probeRow) throws Exception {
			if (buildIter.advanceNext()) {
				if (probeRow != null) {
					innerJoin(buildIter, probeRow);
				} else {
					buildOuterJoin(buildIter);
				}
			} else if (probeRow != null) {
				collect(buildSideNullRow, probeRow);
			}
		}
	}

	/**
	 * Semi join.
	 */
	private static class SemiHashJoinOperator extends HashJoinOperator {

		SemiHashJoinOperator(HashJoinParameter parameter) {
			super(parameter);
		}

		@Override
		public void join(RowIterator<BinaryRow> buildIter, BaseRow probeRow) throws Exception {
			checkNotNull(probeRow);
			if (buildIter.advanceNext()) {
				collector.collect(probeRow);
			}
		}
	}

	/**
	 * Anti join.
	 */
	private static class AntiHashJoinOperator extends HashJoinOperator {

		AntiHashJoinOperator(HashJoinParameter parameter) {
			super(parameter);
		}

		@Override
		public void join(RowIterator<BinaryRow> buildIter, BaseRow probeRow) throws Exception {
			checkNotNull(probeRow);
			if (!buildIter.advanceNext()) {
				collector.collect(probeRow);
			}
		}
	}

	/**
	 * BuildLeftSemiOrAnti join.
	 */
	private static class BuildLeftSemiOrAntiHashJoinOperator extends HashJoinOperator {

		BuildLeftSemiOrAntiHashJoinOperator(HashJoinParameter parameter) {
			super(parameter);
		}

		@Override
		public void join(RowIterator<BinaryRow> buildIter, BaseRow probeRow) throws Exception {
			if (buildIter.advanceNext()) {
				if (probeRow != null) { //Probe phase
					// we must iterator to set probedSet.
					while (buildIter.advanceNext()) {}
				} else { //End Probe phase, iterator build side elements.
					collector.collect(buildIter.getRow());
					while (buildIter.advanceNext()) {
						collector.collect(buildIter.getRow());
					}
				}
			}
		}
	}
}
