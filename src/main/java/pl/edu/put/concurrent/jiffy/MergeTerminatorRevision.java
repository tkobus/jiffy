/*
 * Copyright 2021 Poznan University of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

/*
 * This file is available under and governed by the MIT license.
 *
 * Written by Tadeusz Kobus and Maciej Kokocinski, as a modification
 * of java.util.concurrent.ConcurrentSkipListMap.
 *
 * The original implementation of ConcurrentSkipListMap was written by
 * Doug Lea with assistance from members of JCP JSR-166 Expert Group
 * and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */
package pl.edu.put.concurrent.jiffy;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import pl.edu.put.concurrent.jiffy.SingleMultiVal.MultiValIndices;

class MergeTerminatorRevision<K, V> extends Revision<K, V> {

	Node<K, V> node;
	Node<K, V> mergedIntoNode = null;
	MergeRevision<K, V> mergeRevision = null;
	int indexOfKeyInNextMultiVal; // key to be removed
	int endSizeWithIndices; // used in batches only

	MergeTerminatorRevision(long version, Node<K, V> node, Revision<K, V> next,
			int indexOfKeyInNextMultiVal) {
		super(null, version, null, null, -1, -1, null, next, null);
		this.type = MERGE_TERMINATOR;
		this.node = node;
		this.indexOfKeyInNextMultiVal = indexOfKeyInNextMultiVal;
	}

	public MergeTerminatorRevision(long version, BatchDescriptor<K, V> descriptor, K nodeLeftKey,
			int indexOfLeftmostRelevantBatchKey,
			int indexOfRightmostRelevantBatchKey,
			K rightmostRelevantBatchKey, Node<K, V> node, Revision<K, V> next, 
			MultiValIndices<K> indices) {
		super(null, version, descriptor, nodeLeftKey, 
				indexOfLeftmostRelevantBatchKey,
				indexOfRightmostRelevantBatchKey,
				rightmostRelevantBatchKey, next, indices);
		this.type = MERGE_TERMINATOR;
		this.node = node;
		this.endSizeWithIndices = indices.endSize;
		// this.indices = indices;
	}

	protected void setMergeRevision(MergeRevision<K, V> mergeRevision) {
		MERGE_REVISION.setOpaque(this, mergeRevision);
	}

	protected MergeRevision<K, V> acquireMergeRevision() {
		return (MergeRevision<K, V>) MERGE_REVISION.getOpaque(this);
	}

	protected void setMergedIntoNode(Node<K, V> mergedIntoNode) {
		MERGED_INTO_NODE.setRelease(this, mergedIntoNode);
	}

	protected Node<K, V> acquireMergedIntoNode() {
		return (Node<K, V>) MERGED_INTO_NODE.getAcquire(this);
	}

	@Override
	public String toString() {
		K nodeLeftKey = null;
		return String.format("(%s) MTRev, desc: %s, leftKey %s (lBi: %d rBi: %d), idx: %d, next: %s, mergeRev: %s, mergedIntoNode: %s",
				myId(), (descriptor == null ? "null" : descriptor.acquireVersion()), nodeLeftKey, indexOfLeftmostRelevantBatchKey, indexOfRightmostRelevantBatchKey, indexOfKeyInNextMultiVal,
				next, mergeRevision, mergedIntoNode);
	}

	// VarHandle mechanics
	private static final VarHandle MERGE_REVISION;
	private static final VarHandle MERGED_INTO_NODE;

	static {
		try {
			MethodHandles.Lookup l = MethodHandles.lookup();
			MERGE_REVISION = l.findVarHandle(MergeTerminatorRevision.class, "mergeRevision",
					MergeRevision.class);
			MERGED_INTO_NODE = l.findVarHandle(MergeTerminatorRevision.class, "mergedIntoNode", Node.class);
		} catch (ReflectiveOperationException e) {
			throw new ExceptionInInitializerError(e);
		}
	}
}
