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

package org.apache.flink.runtime.clusterframework.types;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.resources.Resource;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * Describe the immutable resource profile of the slot, either when requiring or offering it. The profile can be
 * checked whether it can match another profile's requirement, and furthermore we may calculate a matching
 * score to decide which profile we should choose when we have lots of candidate slots.
 * It should be generated from {@link ResourceSpec} with the input and output memory calculated in JobMaster.
 *
 * <p>Resource Profiles have a total ordering, defined by comparing these fields in sequence:
 * <ol>
 *     <li>Memory Size</li>
 *     <li>CPU cores</li>
 *     <li>Extended resources</li>
 * </ol>
 * The extended resources are compared ordered by the resource names.
 */
public class ResourceProfile implements Serializable, Comparable<ResourceProfile> {

	private static final long serialVersionUID = 1L;

	public static final ResourceProfile UNKNOWN = new ResourceProfile(-1.0, -1);

	public static final ResourceProfile EMTPY = new ResourceProfile(0, 0);

	// ------------------------------------------------------------------------

	/** How many cpu cores are needed, use double so we can specify cpu like 0.1. */
	private double cpuCores;

	/** How many heap memory in mb are needed. */
	private int heapMemoryInMB;

	/** How many direct memory in mb are needed. */
	private int directMemoryInMB;

	/** How many native memory in mb are needed. */
	private int nativeMemoryInMB;

	/** Memory used for the task in the slot to communicate with its upstreams. Set by job master. */
	private int networkMemoryInMB;

	/** A extensible field for user specified resources from {@link ResourceSpec}. */
	private Map<String, Resource> extendedResources = new HashMap<>(1);

	// ------------------------------------------------------------------------

	/**
	 * Creates a new ResourceProfile.
	 *
	 * @param cpuCores The number of CPU cores (possibly fractional, i.e., 0.2 cores)
	 * @param heapMemoryInMB The size of the heap memory, in megabytes.
	 * @param directMemoryInMB The size of the direct memory, in megabytes.
	 * @param nativeMemoryInMB The size of the native memory, in megabytes.
	 * @param networkMemoryInMB The size of the memory for input and output, in megabytes.
	 * @param extendedResources The extended resources such as GPU and FPGA
	 */
	public ResourceProfile(
			double cpuCores,
			int heapMemoryInMB,
			int directMemoryInMB,
			int nativeMemoryInMB,
			int networkMemoryInMB,
			Map<String, Resource> extendedResources) {
		this.cpuCores = cpuCores;
		this.heapMemoryInMB = heapMemoryInMB;
		this.directMemoryInMB = directMemoryInMB;
		this.nativeMemoryInMB = nativeMemoryInMB;
		this.networkMemoryInMB = networkMemoryInMB;
		if (extendedResources != null) {
			this.extendedResources.putAll(extendedResources);
		}
	}

	/**
	 * Creates a new simple ResourceProfile used for testing.
	 *
	 * @param cpuCores The number of CPU cores (possibly fractional, i.e., 0.2 cores)
	 * @param heapMemoryInMB The size of the heap memory, in megabytes.
	 */
	public ResourceProfile(double cpuCores, int heapMemoryInMB) {
		this(cpuCores, heapMemoryInMB, 0, 0, 0, Collections.emptyMap());
	}

	/**
	 * Creates a copy of the given ResourceProfile.
	 *
	 * @param other The ResourceProfile to copy.
	 */
	public ResourceProfile(ResourceProfile other) {
		this(other.cpuCores,
				other.heapMemoryInMB,
				other.directMemoryInMB,
				other.nativeMemoryInMB,
				other.networkMemoryInMB,
				other.extendedResources);
	}

	// ------------------------------------------------------------------------

	/**
	 * Get the cpu cores needed.
	 *
	 * @return The cpu cores, 1.0 means a full cpu thread
	 */
	public double getCpuCores() {
		return cpuCores;
	}

	/**
	 * Get the heap memory needed in MB.
	 *
	 * @return The heap memory in MB
	 */
	public int getHeapMemoryInMB() {
		return heapMemoryInMB;
	}

	/**
	 * Get the direct memory needed in MB.
	 *
	 * @return The direct memory in MB
	 */
	public int getDirectMemoryInMB() {
		return directMemoryInMB;
	}

	/**
	 * Get the native memory needed in MB.
	 *
	 * @return The native memory in MB
	 */
	public int getNativeMemoryInMB() {
		return nativeMemoryInMB;
	}

	/**
	 * Get the memory needed for task to communicate with its upstreams and downstreams in MB.
	 * @return The network memory in MB
	 */
	public int getNetworkMemoryInMB() {
		return networkMemoryInMB;
	}

	/**
	 * Get the total memory needed in MB.
	 *
	 * @return The total memory in MB
	 */
	public int getMemoryInMB() {
		return heapMemoryInMB + directMemoryInMB + nativeMemoryInMB + networkMemoryInMB;
	}

	/**
	 * Get the memory the operators needed in MB.
	 *
	 * @return The operator memory in MB
	 */
	public int getOperatorsMemoryInMB() {
		return heapMemoryInMB + directMemoryInMB + nativeMemoryInMB;
	}

	/**
	 * Get the extended resources.
	 *
	 * @return The extended resources
	 */
	public Map<String, Resource> getExtendedResources() {
		return Collections.unmodifiableMap(extendedResources);
	}

	/**
	 * Get the managed memory of task manager.
	 * @return The managed memory in MB
	 */
	public int getManagedMemoryInMB() {
		Resource managedMemory = extendedResources.get(ResourceSpec.MANAGED_MEMORY_NAME);
		if (managedMemory != null) {
			return (int)managedMemory.getValue();
		}

		return 0;
	}

	/**
	 * Get the floating memory needed in MB
	 * @return The floating memory in MB
	 */
	public int getFloatingManagedMemoryInMB() {
		Resource floatingMemory = extendedResources.get(ResourceSpec.FLOATING_MANAGED_MEMORY_NAME);
		if (floatingMemory != null) {
			return (int) floatingMemory.getValue();
		}
		return 0;
	}

	/**
	 * Check whether required resource profile can be matched.
	 *
	 * @param required the required resource profile
	 * @return true if the requirement is matched, otherwise false
	 */
	public boolean isMatching(ResourceProfile required) {
		if (cpuCores >= required.getCpuCores() &&
				heapMemoryInMB >= required.getHeapMemoryInMB() &&
				directMemoryInMB >= required.getDirectMemoryInMB() &&
				nativeMemoryInMB >= required.getNativeMemoryInMB() &&
				networkMemoryInMB >= required.getNetworkMemoryInMB()) {
			for (Map.Entry<String, Resource> resource : required.extendedResources.entrySet()) {
				// Skip floating memory, floating memory will not be considered when findMatchingSlot in slot manager
				if (resource.getKey().equals(ResourceSpec.FLOATING_MANAGED_MEMORY_NAME)) {
					continue;
				}
				if (!extendedResources.containsKey(resource.getKey()) ||
						!extendedResources.get(resource.getKey()).getResourceAggregateType().equals(resource.getValue().getResourceAggregateType()) ||
						extendedResources.get(resource.getKey()).getValue() < resource.getValue().getValue()) {
					return false;
				}
			}
			return true;
		}
		return false;
	}

	@Override
	public int compareTo(@Nonnull ResourceProfile other) {
		int cmp = Integer.compare(this.getMemoryInMB(), other.getMemoryInMB());
		if (cmp == 0) {
			cmp = Double.compare(this.cpuCores, other.cpuCores);
		}
		if (cmp == 0) {
			Iterator<Map.Entry<String, Resource>> thisIterator = extendedResources.entrySet().iterator();
			Iterator<Map.Entry<String, Resource>> otherIterator = other.extendedResources.entrySet().iterator();
			while (thisIterator.hasNext() && otherIterator.hasNext()) {
				Map.Entry<String, Resource> thisResource = thisIterator.next();
				Map.Entry<String, Resource> otherResource = otherIterator.next();
				if ((cmp = otherResource.getKey().compareTo(thisResource.getKey())) != 0) {
					return cmp;
				}
				if (!otherResource.getValue().getResourceAggregateType().equals(thisResource.getValue().getResourceAggregateType())) {
					return 1;
				}
				if ((cmp = Double.compare(thisResource.getValue().getValue(), otherResource.getValue().getValue())) != 0) {
					return cmp;
				}
			}
			if (thisIterator.hasNext()) {
				return 1;
			}
			if (otherIterator.hasNext()) {
				return -1;
			}
		}
		return cmp;
	}

	/**
	 * Compute the result of subtract another resource from this piece of resource. The extended resource not included in this
	 * resource will cause IllegalArgumentException.
	 *
	 * This method do not check whether enough resource is present. The caller of this method is responsible to do the checking.
	 *
	 * @param another The resource to subtract.
	 * @return The result of subtract another resource from this resource.
	 *
	 * @throws IllegalArgumentException The extended resource not included in this resource will cause IllegalArgumentException.
	 */
	public ResourceProfile minus(ResourceProfile another) {
		for (String extendedResourceName : another.extendedResources.keySet()) {
			if (!extendedResources.containsKey(extendedResourceName)) {
				throw new IllegalArgumentException("Non-exist extended resource: " + extendedResourceName);
			}
		}

		Map<String, Resource> newExtendedResource = new HashMap<String, Resource>(extendedResources.size());

		for (Map.Entry<String, Resource> entry : extendedResources.entrySet()) {
			Resource anotherResource = another.extendedResources.get(entry.getKey());

			if (anotherResource == null) {
				newExtendedResource.put(entry.getKey(), entry.getValue());
			} else {
				newExtendedResource.put(entry.getKey(), entry.getValue().minus(anotherResource));
			}
		}

		return new ResourceProfile(
			cpuCores - another.cpuCores,
			heapMemoryInMB - another.heapMemoryInMB,
			directMemoryInMB - another.directMemoryInMB,
			nativeMemoryInMB - another.nativeMemoryInMB,
			networkMemoryInMB - another.networkMemoryInMB,
			newExtendedResource);
	}

	public ResourceProfile merge(ResourceProfile another) {
		ResourceProfile resourceProfile = new ResourceProfile(this);
		resourceProfile.addTo(another);
		return resourceProfile;
	}

	public void addTo(ResourceProfile another) {
		this.cpuCores += another.getCpuCores();
		this.heapMemoryInMB += another.getHeapMemoryInMB();
		this.directMemoryInMB += another.getDirectMemoryInMB();
		this.nativeMemoryInMB += another.getNativeMemoryInMB();
		this.networkMemoryInMB += another.getNetworkMemoryInMB();
		if (!extendedResources.isEmpty() || !another.extendedResources.isEmpty()) {
			for (Map.Entry<String, Resource> extendResource : another.extendedResources.entrySet()) {
				Resource rfValue = extendedResources.get(extendResource.getKey());
				if (rfValue != null) {
					extendedResources.put(extendResource.getKey(), extendResource.getValue().merge(rfValue));
				} else {
					extendedResources.put(extendResource.getKey(), extendResource.getValue());
				}
			}
		}
	}

	public ResourceProfile multiply(int multiplier) {
		Map<String, Resource> newExtendedResource = new HashMap<>(extendedResources.size());

		for (Map.Entry<String, Resource> entry : extendedResources.entrySet()) {
			newExtendedResource.put(entry.getKey(), entry.getValue().multiply(multiplier));
		}

		return new ResourceProfile(
			this.getCpuCores() * multiplier,
			this.getHeapMemoryInMB() * multiplier,
			this.getDirectMemoryInMB() * multiplier,
			this.getNativeMemoryInMB() * multiplier,
			this.getNetworkMemoryInMB() * multiplier,
			newExtendedResource);
	}

	// ------------------------------------------------------------------------

	@Override
	public int hashCode() {
		final long cpuBits =  Double.doubleToLongBits(cpuCores);
		int result = (int) (cpuBits ^ (cpuBits >>> 32));
		result = 31 * result + heapMemoryInMB;
		result = 31 * result + directMemoryInMB;
		result = 31 * result + nativeMemoryInMB;
		result = 31 * result + networkMemoryInMB;
		result = 31 * result + extendedResources.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		else if (obj != null && obj.getClass() == ResourceProfile.class) {
			ResourceProfile that = (ResourceProfile) obj;
			return this.cpuCores == that.cpuCores &&
					this.heapMemoryInMB == that.heapMemoryInMB &&
					this.directMemoryInMB == that.directMemoryInMB &&
					this.networkMemoryInMB == that.networkMemoryInMB &&
					Objects.equals(extendedResources, that.extendedResources);
		}
		return false;
	}

	@Override
	public String toString() {
		final StringBuilder resources = new StringBuilder(extendedResources.size() * 10);
		for (Map.Entry<String, Resource> resource : extendedResources.entrySet()) {
			resources.append(", ").append(resource.getKey()).append('=').append(resource.getValue().getValue());
		}
		return "ResourceProfile{" +
			"cpuCores=" + cpuCores +
			", heapMemoryInMB=" + heapMemoryInMB +
			", directMemoryInMB=" + directMemoryInMB +
			", nativeMemoryInMB=" + nativeMemoryInMB +
			", networkMemoryInMB=" + networkMemoryInMB + resources +
			'}';
	}

	public static ResourceProfile fromResourceSpec(ResourceSpec resourceSpec, int networkMemory) {
		Map<String, Resource> copiedExtendedResources = new HashMap<>(resourceSpec.getExtendedResources());

		return new ResourceProfile(
				resourceSpec.getCpuCores(),
				resourceSpec.getHeapMemory(),
				resourceSpec.getDirectMemory(),
				resourceSpec.getNativeMemory(),
				networkMemory,
				copiedExtendedResources);
	}
}
