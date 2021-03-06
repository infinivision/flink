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

package org.apache.flink.runtime.state.subkeyed;

import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

/**
 * /**
 * The interface for {@link SubKeyedState}s whose values are a collection of
 * key-value pairs. But different from {@link SubKeyedMapState}, when iterating
 * over the key-value pairs under a given key, the order in which the key-value
 * pairs are iterated is specified via the comparator in the state's descriptor.
 *
 * @param <K> The type of the keys in the state.
 * @param <N> The type of the namespaces in the state.
 * @param <MK> The type of the keys in the mappings.
 * @param <MV> The type of the values in the mappings.
 */
public interface SubKeyedSortedMapState<K, N, MK, MV> extends AbstractSubKeyedMapState<K, N, MK, MV, SortedMap<MK, MV>> {

	/**
	 * Returns the entry with the smallest map key under the given key and namespace.
	 *
	 * @param key The key under which the retrieved entry locates.
	 * @param namespace The namespace under which the retrieved entry locates.
	 * @return The entry with the smallest map key under the given key.
	 */
	Map.Entry<MK, MV> firstEntry(K key, N namespace);

	/**
	 * Returns the entry with the largest map key under the given key and namespace.
	 *
	 * @param key The key under which the retrieved entry locates.
	 * @param namespace The namespace under which the retrieved entry locates.
	 * @return The entry with the largest map key under the given key.
	 */
	Map.Entry<MK, MV> lastEntry(K key, N namespace);

	/**
	 * Returns an iterator over all the mappings under the given key and
	 * namespace whose mapping keys are strictly less than {@code endMapKey}.
	 * The iterator is backed by the state, so changes to the iterator are
	 * reflected in the state, and vice-versa.
	 *
	 * @param key The key under which the mappings are to be iterated.
	 * @param namespace The namespace of the mappings to be iterated.
	 * @param endMapKey The high endpoint (exclusive) of the map keys in the
	 *                  mappings to be iterated.
	 * @return An iterator over all the mappings under the given key and
	 *         namespace whose mapping keys are equal to or greater than
	 *         {@code endMapKey}.
	 */
	Iterator<Map.Entry<MK, MV>> headIterator(K key, N namespace, MK endMapKey);

	/**
	 * Returns an iterator over all the mappings under the given key and
	 * namespace whose mapping keys are equal to or greater than
	 * {@code startMapKey}. The iterator is backed by the state, so changes to
	 * the iterator are reflected in the state, and vice-versa.
	 *
	 * @param key The key under which the mappings are to be iterated.
	 * @param namespace The namespace of the mappings to be iterated.
	 * @param startMapKey The low endpoint (inclusive) of the map keys in the
	 *                    mappings to be iterated.
	 * @return An iterator over all the mappings under the given key and
	 *         namespace whose mapping keys are equal to or greater than
	 *         {@code startMapKey}.
	 */
	Iterator<Map.Entry<MK, MV>> tailIterator(K key, N namespace, MK startMapKey);

	/**
	 * Returns an iterator over all the mappings under the given key and
	 * namespace whose mapping keys locate in the given range. The iterator is
	 * backed by the state, so changes to the iterator are reflected in the
	 * state, and vice-versa.
	 *
	 * @param key The key under which the mappings are to be iterated.
	 * @param namespace The namespace of the mappings to be iterated.
	 * @param startMapKey The low endpoint (inclusive) of the map keys in the
	 *                    mappings to be iterated.
	 * @param endMapKey The high endpoint (exclusive) of the map keys in the
	 *                  mappings to be iterated.
	 * @return An iterator over all the mappings under the given key and
	 *         namespace whose mapping keys locate in the given range.
	 */
	Iterator<Map.Entry<MK, MV>> subIterator(K key, N namespace, MK startMapKey, MK endMapKey);
}

