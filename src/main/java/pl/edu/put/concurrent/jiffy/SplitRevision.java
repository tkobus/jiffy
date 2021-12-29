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

import pl.edu.put.concurrent.jiffy.SingleMultiVal.MultiValIndices;

class SplitRevision<K, V> extends Revision<K, V> {

	SplitRevision<K, V> sibling;
	Node<K, V> node;
	boolean left;
	int levels = -1;

	public SplitRevision(MultiVal<K, V> value, long version, Revision<K, V> next, boolean left) {
		super(value, version, next);
		this.type = SPLIT;
		this.left = left;
	}

	public SplitRevision(MultiVal<K, V> value, 
			long version, 
			BatchDescriptor<K, V> descriptor, 
			K nodeLeftKey,
			int indexOfLeftmostRelevantBatchKey,
			int indexOfRightmostRelevantBatchKey,
			K rightmostRelevantBatchKey, 
			Revision<K, V> next, 
			MultiValIndices<K> nextIndices, 
			boolean left) {
		super(value, version, descriptor, nodeLeftKey, 
				indexOfLeftmostRelevantBatchKey,
				indexOfRightmostRelevantBatchKey,
				rightmostRelevantBatchKey, next, nextIndices);
		this.type = SPLIT;
		this.left = left;
	}

	@Override
	public String toString() {
//		return String.format("SRev %s, ver: %d, val: %s, key/val: %s %s", left ? "l" : "r", version,
//				getValue(), key, keyValue);
		
		// debug
		K nodeLeftKey = null;
		return String.format("[%s SRev %s, ver: %d, desc: %s, leftKey: %s (lBi: %d rBi: %d), val: %s, par: %s]", myId(), left ? "l" : "r",
				version, (descriptor == null ? "null" : descriptor.acquireVersion()), nodeLeftKey, indexOfLeftmostRelevantBatchKey, indexOfRightmostRelevantBatchKey, getValue(), null);
	}

}
