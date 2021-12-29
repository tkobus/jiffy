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

class MergeRevision<K, V> extends Revision<K, V> {

	Revision<K, V> rightNext;
	Node<K, V> nodeToTerminate;
	K keyOfRightNode;
	MergeTerminatorRevision<K, V> mergeTerminator;
	boolean readyToGC = false;

	MergeRevision(MultiVal<K, V> value, long version, Revision<K, V> leftNext, Revision<K, V> rightNext, Node<K, V> nodeToTerminate,
			MergeTerminatorRevision<K, V> mergeTerminator) {
		super(value, version, leftNext);
		this.type = MERGE;
		assert version <= 0;
		this.rightNext = rightNext;
		this.nodeToTerminate = nodeToTerminate;
		this.keyOfRightNode = nodeToTerminate.key;
		this.mergeTerminator = mergeTerminator;
	}
	
	MergeRevision(MultiVal<K, V> value, long version, BatchDescriptor<K, V> descriptor, K nodeLeftKey,
			int indexOfLeftmostRelevantBatchKey,
			int indexOfRightmostRelevantBatchKey,
			K rightmostRelevantBatchKey,
			Revision<K, V> leftNext, MultiValIndices<K> leftIndices, Revision<K, V> rightNext, Node<K, V> nodeToTerminate,
			MergeTerminatorRevision<K, V> mergeTerminator) {
		super(value, version, descriptor, nodeLeftKey, 
				indexOfLeftmostRelevantBatchKey,
				indexOfRightmostRelevantBatchKey,
				rightmostRelevantBatchKey, leftNext, leftIndices);
		this.type = MERGE;
		assert version <= 0;
		this.rightNext = rightNext;
		this.nodeToTerminate = nodeToTerminate;
		this.keyOfRightNode = nodeToTerminate.key;
		this.mergeTerminator = mergeTerminator;
	}

	protected Revision<K, V> acquireRightNext() {
		return (Revision<K, V>) RIGHT_NEXT.getAcquire(this);
	}

	protected void nullRightNext() {
		RIGHT_NEXT.setOpaque(this, null);
	}
	
	protected boolean acquireReadyToGC() {
		return (boolean) READY_TO_GC.getAcquire(this);
	}
	
	protected void setReadyToGC() {
		READY_TO_GC.setOpaque(this, true);
	}

	@Override
	public String toString() {
		// debug
		K nodeLeftKey = null;
		return String.format("(%s) MRev ver: %d, desc: %s, leftKey: %s (lBi: %d rBi: %d), rtGC: %s, val: %s", myId(), version, (descriptor == null ? "null" : descriptor.acquireVersion()), nodeLeftKey, indexOfLeftmostRelevantBatchKey, indexOfRightmostRelevantBatchKey, acquireReadyToGC(), getValue());
	}

	// VarHandle mechanics
	private static final VarHandle RIGHT_NEXT;
	private static final VarHandle READY_TO_GC;

	static {
		try {
			MethodHandles.Lookup l = MethodHandles.lookup();
			RIGHT_NEXT = l.findVarHandle(MergeRevision.class, "rightNext", Revision.class);
			READY_TO_GC = l.findVarHandle(MergeRevision.class, "readyToGC", boolean.class);
		} catch (ReflectiveOperationException e) {
			throw new ExceptionInInitializerError(e);
		}
	}


}
