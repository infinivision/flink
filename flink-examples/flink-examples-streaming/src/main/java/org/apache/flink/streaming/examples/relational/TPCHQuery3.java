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

package org.apache.flink.streaming.examples.relational;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.io.TupleCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This program implements a modified version of the TPC-H query 3. The
 * example demonstrates how to assign names to fields by extending the Tuple class.
 * The original query can be found at
 * <a href="http://www.tpc.org/tpch/spec/tpch2.16.0.pdf">http://www.tpc.org/tpch/spec/tpch2.16.0.pdf</a> (page 29).
 *
 * <p>This program implements the following SQL equivalent:
 *
 * <p><pre>{@code
 * SELECT
 *      l_orderkey,
 *      SUM(l_extendedprice*(1-l_discount)) AS revenue,
 *      o_orderdate,
 *      o_shippriority
 * FROM customer,
 *      orders,
 *      lineitem
 * WHERE
 *      c_mktsegment = '[SEGMENT]'
 *      AND c_custkey = o_custkey
 *      AND l_orderkey = o_orderkey
 *      AND o_orderdate < date '[DATE]'
 *      AND l_shipdate > date '[DATE]'
 * GROUP BY
 *      l_orderkey,
 *      o_orderdate,
 *      o_shippriority;
 * }*</pre>
 *
 * <p>Compared to the original TPC-H query this version does not sort the result by revenue
 * and orderdate.
 *
 * <p>Input files are plain text CSV files using the pipe character ('|') as field separator
 * as generated by the TPC-H data generator which is available at <a href="http://www.tpc.org/tpch/">http://www.tpc.org/tpch/</a>.
 *
 * <p>Usage: <code>TPCHQuery3 --lineitem&lt;path&gt; --customer &lt;path&gt; --orders&lt;path&gt; --output &lt;path&gt;</code><br>
 *
 * <p>This example shows how to use:
 * <ul>
 * <li> custom data type derived from tuple data types
 * <li> inline-defined functions
 * <li> build-in aggregation functions
 * </ul>
 */
@SuppressWarnings("serial")
public class TPCHQuery3 {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	/**
	 * The entry point of application.
	 *
	 * @param args the input arguments
	 * @throws Exception the exception
	 */
	public static void main(String[] args) throws Exception {
		final long start = System.currentTimeMillis();

		final ParameterTool params = ParameterTool.fromArgs(args);

		if (!params.has("lineitem") && !params.has("customer") && !params.has("orders")) {
			System.err.println("  This program expects data from the TPC-H benchmark as input data.");
			System.err.println("  Due to legal restrictions, we can not ship generated data.");
			System.out.println("  You can find the TPC-H data generator at http://www.tpc.org/tpch/.");
			System.out.println("  Usage: TPCHQuery3 --lineitem <path> --customer <path> --orders <path> [--output <path>]");
			return;
		}

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		env.getConfig().setGlobalJobParameters(params);

		if (params.has("object-reuse")) {
			env.getConfig().enableObjectReuse();
		} else {
			env.getConfig().disableObjectReuse();
		}

		boolean useSourceV2 = params.has("sourcev2");

		if (params.has("multiple-chain")) {
			env.setMultiHeadChainMode(true);
			useSourceV2 = true;
		} else {
			env.setMultiHeadChainMode(false);
		}

		int customParallelism = params.getInt("custom-parallelism", 1);
		int orderParallelism = params.getInt("order-parallelism", 1);
		int lineitemParallelism = params.getInt("lineitem-parallelism", 1);
		int groupbyParallelism = params.getInt("groupby-parallelism", 1);

		double customCpu = params.getDouble("custom-cpu", 1);
		double orderCpu = params.getDouble("order-cpu", 1);
		double lineitemCpu = params.getDouble("lineitem-cpu", 1);
		double filterCpu = params.getDouble("filter-cpu", 1);
		double joinCpu = params.getDouble("join-cpu", 1);
		double groupbyCpu = params.getDouble("groupby-cpu", 1);
		double sinkCpu = params.getDouble("sink-cpu", 1);

		int customMem = params.getInt("custom-mem", 256);
		int orderMem = params.getInt("order-mem", 256);
		int lineitemMem = params.getInt("lineitem-mem", 256);
		int filterMem = params.getInt("filter-mem", 256);
		int join1Mem = params.getInt("join1-mem", 1024);
		int join2Mem = params.getInt("join2-mem", 2048);
		int groupbyMem = params.getInt("groupby-mem", 256);
		int sinkMem = params.getInt("sink-mem", 256);

		// get input data
		SingleOutputStreamOperator<Lineitem> lineitems = getLineitemDataStream(env, params.get("lineitem"), useSourceV2).
			setParallelism(lineitemParallelism).
			setResources(
				new ResourceSpec.Builder().
					setCpuCores(lineitemCpu).
					setHeapMemoryInMB(lineitemMem).
					build());
		SingleOutputStreamOperator<Customer> customers = getCustomerDataStream(env, params.get("customer"), useSourceV2).
			setParallelism(customParallelism).
			setResources(
				new ResourceSpec.Builder().
					setCpuCores(customCpu).
					setHeapMemoryInMB(customMem).
					build());
		SingleOutputStreamOperator<Order> orders = getOrdersDataStream(env, params.get("orders"), useSourceV2).
			setParallelism(orderParallelism).
			setResources(
				new ResourceSpec.Builder().
					setCpuCores(orderCpu).
					setHeapMemoryInMB(orderMem).
					build());

		// Filter market segment "AUTOMOBILE"
		customers = customers.
			filter(
				(FilterFunction<Customer>) c -> c.getMktsegment().equals("AUTOMOBILE")).
			setParallelism(customParallelism).
			setResources(
				new ResourceSpec.Builder().
					setCpuCores(filterCpu).
					setHeapMemoryInMB(filterMem).
					build());

		// Filter all Orders with o_orderdate < 12.03.1995
		orders = orders.filter(
							new FilterFunction<Order>() {
								private final DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
								private final Date date = format.parse("1995-03-12");

								@Override
								public boolean filter(Order o) throws ParseException {
									return format.parse(o.getOrderdate()).before(date);
								}
							}).
			setParallelism(orderParallelism).
			setResources(
				new ResourceSpec.Builder().
					setCpuCores(filterCpu).
					setHeapMemoryInMB(filterMem).
					build());

		// Filter all Lineitems with l_shipdate > 12.03.1995
		lineitems = lineitems.filter(
								new FilterFunction<Lineitem>() {
									private final DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
									private final Date date = format.parse("1995-03-12");

									@Override
									public boolean filter(Lineitem l) throws ParseException {
										return format.parse(l.getShipdate()).after(date);
									}
								}).
			setParallelism(lineitemParallelism).
			setResources(
				new ResourceSpec.Builder().
					setCpuCores(filterCpu).
					setHeapMemoryInMB(filterMem).
					build());

		// Join customers with orders and package them into a ShippingPriorityItem
		final ConnectedStreams<Customer, Order> customerJoinedOrderStream =
			customers.
			broadcast().
			connect(orders);

		DataStream<ShippingPriorityItem> customerWithOrders =
			coFlatMapWithPriority(customerJoinedOrderStream, new CustomerOrderJoinFunction()).
			setParallelism(orderParallelism).
			setResources(
				new ResourceSpec.Builder().
					setCpuCores(joinCpu).
					setHeapMemoryInMB(join1Mem).
					build());

		// Join the last join result with Lineitems
		final ConnectedStreams<ShippingPriorityItem, Lineitem> orderJoinedLineitemStream =
			customerWithOrders.
			broadcast().
			connect(lineitems);

		DataStream<ShippingPriorityItem> result =
			coFlatMapWithPriority(orderJoinedLineitemStream, new LineitemOrderJoinFunction()).
			setParallelism(lineitemParallelism).
			setResources(
				new ResourceSpec.Builder().
					setCpuCores(joinCpu).
					setHeapMemoryInMB(join2Mem).
					build()).
			keyBy(0, 2, 3).
			sum(1).
			setParallelism(groupbyParallelism).
			setResources(
				new ResourceSpec.Builder().
					setCpuCores(groupbyCpu).
					setHeapMemoryInMB(groupbyMem).
					build());

		final DataStreamSink<ShippingPriorityItem> sink;
		// emit result
		if (params.has("output")) {
			sink = result.writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE, "\n", "|");
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			sink = result.print();
		}

		sink.setParallelism(groupbyParallelism).
			setResources(
				new ResourceSpec.Builder().
					setCpuCores(sinkCpu).
					setHeapMemoryInMB(sinkMem).
					build());

		// execute program
		env.execute("TPCH Query 3 Example");

		System.out.println("Used: " + (System.currentTimeMillis() - start));
	}

	// *************************************************************************
	//     DATA TYPES
	// *************************************************************************

	/**
	 * Join function for custom and order.
	 */
	public static class CustomerOrderJoinFunction extends RichCoFlatMapFunction<Customer, Order, ShippingPriorityItem> {

		private transient Set<Long> customKeys;

		private transient boolean objectReuse;

		private transient ShippingPriorityItem reusedObject;

		private transient Accumulator<Long, Long> customBeforeJoin;
		private transient Accumulator<Long, Long> orderBeforeJoin;
		private transient Accumulator<Long, Long> customJoinedOrder;
		private transient Accumulator<Long, Long> broadcastCost;

		@Override
		public void open(Configuration parameters) throws Exception {
			customKeys = new HashSet<>();
			objectReuse = getRuntimeContext().getExecutionConfig().isObjectReuseEnabled();
			reusedObject = new ShippingPriorityItem();

			getRuntimeContext().addAccumulator("customBeforeJoin", new LongCounter());
			customBeforeJoin = getRuntimeContext().getAccumulator("customBeforeJoin");

			getRuntimeContext().addAccumulator("orderBeforeJoin", new LongCounter());
			orderBeforeJoin = getRuntimeContext().getAccumulator("orderBeforeJoin");

			getRuntimeContext().addAccumulator("customJoinedOrder", new LongCounter());
			customJoinedOrder = getRuntimeContext().getAccumulator("customJoinedOrder");

			getRuntimeContext().addAccumulator("broadcastCost", new LongCounter());
			broadcastCost = getRuntimeContext().getAccumulator("broadcastCost");
		}

		@Override
		public void flatMap1(Customer customer, Collector<ShippingPriorityItem> collector) throws Exception {
			if (objectReuse) {
				//noinspection BoxingBoxedValue
				customKeys.add(new Long(customer.getCustKey()));
			} else {
				customKeys.add(customer.getCustKey());
			}
			customBeforeJoin.add(1L);
		}

		@Override
		public void flatMap2(Order order, Collector<ShippingPriorityItem> collector) throws Exception {
			if (customKeys.contains(order.getCustKey())) {
				long start = System.currentTimeMillis();
				if (objectReuse) {
					collector.collect(
						new ShippingPriorityItem(
							order.getOrderKey(),
							0.0,
							order.getOrderdate(),
							order.getShippriority()));
				} else {
					reusedObject.setOrderkey(order.getOrderKey());
					reusedObject.setRevenue(0.0);
					reusedObject.setOrderdate(order.getOrderdate());
					reusedObject.setShippriority(order.getShippriority());
					collector.collect(reusedObject);
				}
				broadcastCost.add(System.currentTimeMillis() - start);
				customJoinedOrder.add(1L);
			}
			orderBeforeJoin.add(1L);
		}

		@Override
		public void close() {
			this.customKeys.clear();
			this.customKeys = null;
			System.out.println("custom=" + customBeforeJoin.getLocalValue() +
				", order=" + orderBeforeJoin.getLocalValue() +
				", joined=" + customJoinedOrder.getLocalValue());
		}
	}

	/**
	 * Join function for order and lineitem.
	 */
	public static class OrderLineitemJoinFunction extends RichCoFlatMapFunction<Lineitem, ShippingPriorityItem, ShippingPriorityItem> {

		private transient Map<Long, Lineitem> lineitems;

		private transient boolean objectReuse;

		private transient long lineitemCount = 0;
		private transient long orderCount = 0;
		private transient long joinedCount = 0;

		@Override
		public void open(Configuration parameters) throws Exception {
			lineitems = new HashMap<>();
			objectReuse = getRuntimeContext().getExecutionConfig().isObjectReuseEnabled();
		}

		@Override
		public void flatMap1(Lineitem lineitem, Collector<ShippingPriorityItem> collector) throws Exception {
			if (objectReuse) {
				lineitems.put(lineitem.getOrderkey(), new Lineitem(lineitem));
			} else {
				lineitems.put(lineitem.getOrderkey(), lineitem);
			}
			lineitemCount++;
		}

		@Override
		public void flatMap2(ShippingPriorityItem priorityItem, Collector<ShippingPriorityItem> collector) throws Exception {
			final Lineitem lineitem = lineitems.get(priorityItem.getOrderkey());
			if (lineitem != null) {
				if (objectReuse) {
					collector.collect(
						new ShippingPriorityItem(
							priorityItem.f0,
							lineitem.getExtendedprice() * (1 - lineitem.getDiscount()),
							priorityItem.f2,
							priorityItem.f3));
				} else {
					priorityItem.setRevenue(lineitem.getExtendedprice() * (1 - lineitem.getDiscount()));
					collector.collect(priorityItem);
				}
				joinedCount++;
			}
			orderCount++;
		}

		@Override
		public void close() {
			this.lineitems.clear();
			this.lineitems = null;
			System.out.println("lineitem=" + lineitemCount + ", order=" + orderCount + ", joined=" + joinedCount);
		}
	}

	/**
	 * Join function for order and lineitem.
	 */
	public static class LineitemOrderJoinFunction extends RichCoFlatMapFunction<ShippingPriorityItem, Lineitem, ShippingPriorityItem> {

		private transient Map<Long, ShippingPriorityItem> orders;

		private transient boolean objectReuse;

		private transient Accumulator<Long, Long> customJoinedOrderWithoutLineitem;
		private transient Accumulator<Long, Long> lineitemBeforeJoin;
		private transient Accumulator<Long, Long> lineitemJoinedOthers;
		private transient Accumulator<Long, Long> broadcastCost;

		@Override
		public void open(Configuration parameters) throws Exception {
			orders = new HashMap<>();
			objectReuse = getRuntimeContext().getExecutionConfig().isObjectReuseEnabled();

			getRuntimeContext().addAccumulator("customJoinedOrderWithoutLineitem", new LongCounter());
			customJoinedOrderWithoutLineitem = getRuntimeContext().getAccumulator("customJoinedOrderWithoutLineitem");

			getRuntimeContext().addAccumulator("lineitemBeforeJoin", new LongCounter());
			lineitemBeforeJoin = getRuntimeContext().getAccumulator("lineitemBeforeJoin");

			getRuntimeContext().addAccumulator("lineitemJoinedOthers", new LongCounter());
			lineitemJoinedOthers = getRuntimeContext().getAccumulator("lineitemJoinedOthers");

			getRuntimeContext().addAccumulator("broadcastCost", new LongCounter());
			broadcastCost = getRuntimeContext().getAccumulator("broadcastCost");
		}

		@Override
		public void close() {
			System.out.println("lineitem=" + lineitemBeforeJoin.getLocalValue() +
				", order=" + customJoinedOrderWithoutLineitem.getLocalValue() +
				", joined=" + lineitemJoinedOthers.getLocalValue() +
				", orders in map=" + orders.size());
			this.orders.clear();
			this.orders = null;
		}

		@Override
		public void flatMap1(ShippingPriorityItem priorityItem, Collector<ShippingPriorityItem> out) throws Exception {
			if (objectReuse) {
				orders.put(priorityItem.getOrderkey(), new ShippingPriorityItem(priorityItem));
			} else {
				orders.put(priorityItem.getOrderkey(), priorityItem);
			}
			customJoinedOrderWithoutLineitem.add(1L);
		}

		@Override
		public void flatMap2(Lineitem lineitem, Collector<ShippingPriorityItem> collector) throws Exception {
			final ShippingPriorityItem priorityItem = orders.get(lineitem.getOrderkey());
			if (priorityItem != null) {
				long start = System.currentTimeMillis();
				if (objectReuse) {
					collector.collect(
						new ShippingPriorityItem(
							priorityItem.f0,
							lineitem.getExtendedprice() * (1 - lineitem.getDiscount()),
							priorityItem.f2,
							priorityItem.f3));
				} else {
					priorityItem.setRevenue(lineitem.getExtendedprice() * (1 - lineitem.getDiscount()));
					collector.collect(priorityItem);
				}
				broadcastCost.add(System.currentTimeMillis() - start);
				lineitemJoinedOthers.add(1L);
			}
			lineitemBeforeJoin.add(1L);
		}
	}

	/**
	 * Lineitem.
	 */
	public static class Lineitem extends Tuple4<Long, Double, Double, String> {

		/**
		 * Instantiates a new Lineitem.
		 */
		public Lineitem() {

		}

		/**
		 * Instantiates a new Lineitem.
		 *
		 * @param lineitem the lineitem
		 */
		public Lineitem(Lineitem lineitem) {
			super(lineitem.f0, lineitem.f1, lineitem.f2, lineitem.f3);
		}

		/**
		 * Gets orderkey.
		 *
		 * @return the orderkey
		 */
		public Long getOrderkey() {
			return this.f0;
		}

		/**
		 * Gets discount.
		 *
		 * @return the discount
		 */
		public Double getDiscount() {
			return this.f2;
		}

		/**
		 * Gets extendedprice.
		 *
		 * @return the extendedprice
		 */
		public Double getExtendedprice() {
			return this.f1;
		}

		/**
		 * Gets shipdate.
		 *
		 * @return the shipdate
		 */
		public String getShipdate() {
			return this.f3;
		}

	}

	/**
	 * Customer.
	 */
	public static class Customer extends Tuple2<Long, String> {

		/**
		 * Gets cust key.
		 *
		 * @return the cust key
		 */
		public Long getCustKey() {
			return this.f0;
		}

		/**
		 * Gets mktsegment.
		 *
		 * @return the mktsegment
		 */
		public String getMktsegment() {
			return this.f1;
		}
	}

	/**
	 * Order.
	 */
	public static class Order extends Tuple4<Long, Long, String, Long> {

		/**
		 * Gets order key.
		 *
		 * @return the order key
		 */
		public Long getOrderKey() {
			return this.f0;
		}

		/**
		 * Gets cust key.
		 *
		 * @return the cust key
		 */
		public Long getCustKey() {
			return this.f1;
		}

		/**
		 * Gets orderdate.
		 *
		 * @return the orderdate
		 */
		public String getOrderdate() {
			return this.f2;
		}

		/**
		 * Gets shippriority.
		 *
		 * @return the shippriority
		 */
		public Long getShippriority() {
			return this.f3;
		}
	}

	/**
	 * The type Shipping priority item.
	 */
	public static class ShippingPriorityItem extends Tuple4<Long, Double, String, Long> {

		/**
		 * Instantiates a new Shipping priority item.
		 */
		public ShippingPriorityItem() {}

		/**
		 * Instantiates a new Shipping priority item.
		 *
		 * @param orderkey     the orderkey
		 * @param revenue      the revenue
		 * @param orderdate    the orderdate
		 * @param shippriority the shippriority
		 */
		public ShippingPriorityItem(Long orderkey, Double revenue,
				String orderdate, Long shippriority) {
			this.f0 = orderkey;
			this.f1 = revenue;
			this.f2 = orderdate;
			this.f3 = shippriority;
		}

		/**
		 * Instantiates a new Shipping priority item.
		 *
		 * @param other the other
		 */
		public ShippingPriorityItem(ShippingPriorityItem other) {
			this(other.f0, other.f1, other.f2, other.f3);
		}

		/**
		 * Gets orderkey.
		 *
		 * @return the orderkey
		 */
		public Long getOrderkey() {
			return this.f0;
		}

		/**
		 * Sets orderkey.
		 *
		 * @param orderkey the orderkey
		 */
		public void setOrderkey(Long orderkey) {
			this.f0 = orderkey;
		}

		/**
		 * Gets revenue.
		 *
		 * @return the revenue
		 */
		public Double getRevenue() {
			return this.f1;
		}

		/**
		 * Sets revenue.
		 *
		 * @param revenue the revenue
		 */
		public void setRevenue(Double revenue) {
			this.f1 = revenue;
		}

		/**
		 * Gets orderdate.
		 *
		 * @return the orderdate
		 */
		public String getOrderdate() {
			return this.f2;
		}

		/**
		 * Sets orderdate.
		 *
		 * @param orderdate the orderdate
		 */
		public void setOrderdate(String orderdate) {
			this.f2 = orderdate;
		}

		/**
		 * Gets shippriority.
		 *
		 * @return the shippriority
		 */
		public Long getShippriority() {
			return this.f3;
		}

		/**
		 * Sets shippriority.
		 *
		 * @param shippriority the shippriority
		 */
		public void setShippriority(Long shippriority) {
			this.f3 = shippriority;
		}
	}

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static SingleOutputStreamOperator<Lineitem> getLineitemDataStream(StreamExecutionEnvironment env, String lineitemPath, boolean useSourceV2) {
		final CsvReader csvReader =
			new CsvReader(lineitemPath, ExecutionEnvironment.getExecutionEnvironment())
				.fieldDelimiter("|")
				.includeFields("1000011000100000");
		final TupleCsvInputFormat<Lineitem> inputFormat = csvReader.generateTupleCsvInputFormat(Lineitem.class);
		if (useSourceV2) {
			return env.createInputV2(inputFormat, inputFormat.getTupleTypeInfo(), "Lineitem source v2");
		} else {
			return env.createInput(inputFormat, inputFormat.getTupleTypeInfo(), "Lineitem source v1");
		}
	}

	private static SingleOutputStreamOperator<Customer> getCustomerDataStream(StreamExecutionEnvironment env, String customerPath, boolean useSourceV2) {
		final CsvReader csvReader =
			new CsvReader(customerPath, ExecutionEnvironment.getExecutionEnvironment())
					.fieldDelimiter("|")
					.includeFields("10000010");
		final TupleCsvInputFormat<Customer> inputFormat = csvReader.generateTupleCsvInputFormat(Customer.class);
		if (useSourceV2) {
			return env.createInputV2(inputFormat, inputFormat.getTupleTypeInfo(), "Custom source v2");
		} else {
			return env.createInput(inputFormat, inputFormat.getTupleTypeInfo(), "Custom source v1");
		}
	}

	private static SingleOutputStreamOperator<Order> getOrdersDataStream(StreamExecutionEnvironment env, String ordersPath, boolean useSourceV2) {
		final CsvReader csvReader =
			new CsvReader(ordersPath, ExecutionEnvironment.getExecutionEnvironment())
					.fieldDelimiter("|")
					.includeFields("110010010");
		final TupleCsvInputFormat<Order> inputFormat = csvReader.generateTupleCsvInputFormat(Order.class);
		if (useSourceV2) {
			return env.createInputV2(inputFormat, inputFormat.getTupleTypeInfo(), "Order source v2");
		} else {
			return env.createInput(inputFormat, inputFormat.getTupleTypeInfo(), "Order source v1");
		}
	}

	private static <IN1, IN2, R> SingleOutputStreamOperator<R> coFlatMapWithPriority(
		ConnectedStreams<IN1, IN2> connectedStreams,
		CoFlatMapFunction<IN1, IN2, R> coFlatMapper) {

		TypeInformation<R> outTypeInfo = TypeExtractor.getBinaryOperatorReturnType(
			coFlatMapper,
			CoFlatMapFunction.class,
			0,
			1,
			2,
			TypeExtractor.NO_INDEX,
			TypeExtractor.NO_INDEX,
			TypeExtractor.NO_INDEX,
			connectedStreams.getType1(),
			connectedStreams.getType2(),
			Utils.getCallLocationName(),
			true);

		return connectedStreams.transform(
			"Co-Flat Map",
			outTypeInfo,
			new PriorityCoStreamFlatMap<>(connectedStreams.getExecutionEnvironment().clean(coFlatMapper)));
	}
}
