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

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import pl.edu.put.concurrent.MultiversionNavigableMapException;
import pl.edu.put.utils.Pair;

class SingleMultiVal<K, V> implements MultiVal<K, V> {
	private static final boolean DEBUG = false;
	private static final boolean USE_FINGERPRINTS = true;
	private static final boolean FINGERPRINTS_STATISTICS = true;

	static ThreadLocal<long[]> fingerprintStats = new ThreadLocal<>() {
		// 0 - gets
		// 1 - gets null
		// 2 - gets value
		// 3 - get collision
		// 3 - removes
		// 4 - removes fast
		// 5 - removes fast null

		@Override
		protected long[] initialValue() {
			return new long[4];
		}
	};

	public static long[] getBloomStats() {
		return fingerprintStats.get();
	}

	Comparator<? super K> comparator;

	Object[] keys = null;
	Object[] values = null;
	short[] hashes = null;
	short[] indices = null;

	public SingleMultiVal(boolean initializeArrays) {
		if (initializeArrays) {
			keys = new Object[0];
			values = new Object[0];

			if (USE_FINGERPRINTS) {
				hashes = new short[0];
				indices = new short[0];
			}
		}
	}

	public SingleMultiVal(Comparator<? super K> comparator) {
		this.comparator = comparator;

		if (USE_FINGERPRINTS) {
			hashes = new short[0];
			indices = new short[0];
		}
	}

	public SingleMultiVal(Comparator<? super K> comparator, K key, V value) {
		this.comparator = comparator;
		keys = new Object[] { key };
		values = new Object[] { value };

		if (USE_FINGERPRINTS) {
			hashes = new short[] { getHash(key) };
			recalculateIndices();
			if (DEBUG)
				System.out.println("FRESH: " + fpString());
		}
	}

	public SingleMultiVal(Comparator<? super K> comparator, MultiVal<K, V> mvalLeft, MultiVal<K, V> mvalRight,
			int indexOfKeyInNextMultiVal) {
		this.comparator = comparator;

		mergeAndRemove(mvalLeft, mvalRight, indexOfKeyInNextMultiVal);
	}

	public SingleMultiVal(Comparator<? super K> comparator, Batch<K, V> batch, MultiVal<K, V> leftMval,
			MultiVal<K, V> rightMval, MultiValIndices<K> leftIndices, MultiValIndices<K> rightIndices) {
		this.comparator = comparator;

		mergeAndAdd(comparator, batch, leftMval, rightMval, leftIndices, rightIndices);
	}

	@Override
	public SingleMultiVal<K, V> clone() {
		SingleMultiVal<K, V> clone = new SingleMultiVal<>(false);

		if (keys.length == 0) {
			clone.keys = new Object[0];
			clone.values = new Object[0];

			if (USE_FINGERPRINTS) {
				clone.hashes = new short[0];
				clone.indices = new short[0];
			}

			return clone;
		}

		clone.keys = new Object[this.keys.length];
		clone.values = new Object[this.keys.length];

		for (int i = 0; i < keys.length; i++) {
			clone.keys[i] = keys[i];
			clone.values[i] = values[i];
		}

		if (USE_FINGERPRINTS) {
			clone.hashes = Arrays.copyOf(hashes, hashes.length);
			clone.indices = Arrays.copyOf(indices, indices.length);
		}

		return clone;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	static int cpr(Comparator c, Object x, Object y) {
		return (c != null) ? c.compare(x, y) : ((Comparable) x).compareTo(y);
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object o) {
		if (o == this)
			return true;
		if (!(o instanceof SingleMultiVal))
			return false;
		SingleMultiVal<K, V> other = (SingleMultiVal<K, V>) o;

		if (!Arrays.equals(this.keys, other.keys) || !Arrays.equals(this.values, other.values))
			return false;

		return true;
	}

	@Override
	@SuppressWarnings("unchecked")
	public K firstKey() {
		if (keys.length == 0)
			return null;
		return (K) keys[0];
	}

	@Override
	@SuppressWarnings("unchecked")
	public K lastKey() {
		if (keys.length == 0)
			return null;
		return (K) keys[keys.length - 1];
	}

	@Override
	@SuppressWarnings("unchecked")
	public V firstValue() {
		return keys.length == 0 ? null : (V) values[0];
	}

	@Override
	@SuppressWarnings("unchecked")
	public V lastValue() {
		return values.length == 0 ? null : (V) values[values.length - 1];
	}

	@Override
	public int size() {
		return keys.length;
	}

	@Override
	public boolean containsKey(K key) {
		if (USE_FINGERPRINTS) {
			int index = getIndexByHash(key);
			if (index >= 0)
				return true;
			if (index == -1)
				return false;
		}

		return indexOfKeyInMultiVal(key) >= 0;
	}

	// index >= 0 - regular index
	// -1 - slot empty
	// -2 - collision
	private int getIndexByHash(K key) {
		int ret = -1;
		if (keys.length == 0)
			return -1;

		short x = getMicroHash(key);

		short index = (short) (indices[2 * x] - 1);

		if (index == -1) {
			ret = -1;
		} else if (cpr(comparator, key, keys[index]) != 0) {
			index = (short) (indices[2 * x + 1] - 1);
			if (index == -1)
				ret = -1;
			else if (cpr(comparator, key, keys[index]) == 0)
				ret = index;
			else
				ret = -2;
		} else {
			ret = index;
		}

		return ret;
	}

	private short getHash(K key) {
		return (short) (key.hashCode() % ((1 << Short.SIZE) - 1));
	}

	private short getMicroHash(K key) {
		return (short) Math.abs(getHash(key) % keys.length);
	}

	private short getMicroHash(short hash) {
		return (short) Math.abs(hash % keys.length);
	}

	@Override
	public boolean containsValue(V value) {
		if (keys.length == 0)
			return false;

		for (Object o : values) {
			if (value.equals(o))
				return true;
		}

		return false;
	}

	@Override
	public Object[] getKeys() {
		return keys;
	}

	@Override
	public Object[] getValues() {
		return values;
	}

	// Based on open JDK 8 code Arrays.binarySearch
	public int binarySearchKey(K key) {
		int low = 0;
		int high = keys.length - 1;

		while (low <= high) {
			int mid = (low + high) >>> 1;
			Object midVal = keys[mid];
			@SuppressWarnings("unchecked")
			int cmp = cpr(comparator, key, (K) midVal);
			if (cmp > 0)
				low = mid + 1;
			else if (cmp < 0)
				high = mid - 1;
			else
				return mid; // key found
		}

		return -(low + 1); // key not found.
	}

	@Override
	public int indexOfKeyInMultiVal(K key) {
		if (USE_FINGERPRINTS) {
			int index = getIndexByHash(key);
			if (index >= 0)
				return index;
		}

		return binarySearchKey(key);
	}

	public int indexOfKeyInMultiVal(K key, boolean fastPath) {
		if (USE_FINGERPRINTS) {
			int index = getIndexByHash(key);
			if (index >= -1)
				return index;
		}

		return binarySearchKey(key);
	}

	public int indexOfKeyInMultiVal(K key, int low, int high) {
		while (low <= high) {
			int mid = (low + high) >>> 1;
			Object midVal = keys[mid];
			@SuppressWarnings("unchecked")
			int cmp = cpr(comparator, key, (K) midVal);
			if (cmp > 0)
				low = mid + 1;
			else if (cmp < 0)
				high = mid - 1;
			else
				return mid; // key found
		}

		return -(low + 1); // key not found.
	}

	private void indicesOfKeysInMultiVal(Batch<K, V> batch, int firstIndex, int lastIndex, MultiValIndices<K> ret) {
		throw new UnsupportedOperationException();
	}

	private void indicesOfKeysInMultiValFingerprints(Batch<K, V> batch, int firstIndex, int lastIndex,
			MultiValIndices<K> ret) {
		int size = lastIndex - firstIndex + 1;
		int indicesIndex = 0;
		ret.indices = new int[size];
		for (int i = firstIndex; i <= lastIndex; i++) {
			K key = batch.getKeyByIndex(i);
			V value = batch.getValueByIndex(i);
			int index = indexOfKeyInMultiVal(key, true);
			if (!(value == null && index < 0)) {
				// value != null || index >= 0
				// we have to modify something
				if (index == -1)
					index = binarySearchKey(key);

				if (index >= 0 && value == null)
					ret.endSize--;
				else if (index < 0 && value != null)
					ret.endSize++;

				if (value != null)
					ret.newValues++;
			}
			ret.indices[indicesIndex] = index;
			indicesIndex++;
		}
	}

	public static class MultiValIndices<K> {
		int[] indices = new int[0];
		int endSize = 0;
		int newValues = 0;
		int indexOfFirstRelevantKeyInBatch;

		public String toString() {
			// DEBUG
			List<K> keys = null;
			return String.format("%d %d %s %s %d", endSize, newValues, keys, Arrays.toString(indices), indexOfFirstRelevantKeyInBatch);
		}
	}

	public MultiValIndices<K> indexOfKeysInMultiVal(Batch<K, V> batch, int indexOfFirstRelevantBatchKeyFromRight, K nodeKey) {
		MultiValIndices<K> ret = new MultiValIndices<>();
		ret.endSize = keys.length;

		K firstRelevantBatchKeyFromRight = indexOfFirstRelevantBatchKeyFromRight == -1 ? null : batch.getKeyByIndex(indexOfFirstRelevantBatchKeyFromRight);
		
		if (firstRelevantBatchKeyFromRight == null
				|| (nodeKey != null && cpr(comparator, nodeKey, firstRelevantBatchKeyFromRight) > 0)) 
			return ret;

		int firstIndex = 0;
		if (nodeKey != null && indexOfFirstRelevantBatchKeyFromRight > 0) {
			K immediateLeftKey = batch.getKeyByIndex(indexOfFirstRelevantBatchKeyFromRight - 1);
			if (cpr(comparator, immediateLeftKey, nodeKey) < 0)
				firstIndex = indexOfFirstRelevantBatchKeyFromRight;
			else
				firstIndex = batch.ceilingKeyIndex(nodeKey);
		}
		
		ret.indexOfFirstRelevantKeyInBatch = firstIndex;
		int lastIndex = indexOfFirstRelevantBatchKeyFromRight;

		int headMapSize = lastIndex - firstIndex + 1;
		if (headMapSize <= 0)
			return ret;

		if (headMapSize == 1) {
			K key = batch.getKeyByIndex(firstIndex);
			V value = batch.getValueByIndex(firstIndex);
			int index = indexOfKeyInMultiVal(key, true);
			if (!(value == null && index < 0)) {
				if (index == -1)
					index = binarySearchKey(key);

				if (index >= 0 && value == null)
					ret.endSize--;
				else if (index < 0 && value != null)
					ret.endSize++;

				if (value != null)
					ret.newValues++;
			}
			ret.indices = new int[] { index };
		} else {
			if (USE_FINGERPRINTS)
				indicesOfKeysInMultiValFingerprints(batch, firstIndex, lastIndex, ret);
			else
				indicesOfKeysInMultiVal(batch, firstIndex, lastIndex, ret);
		}
		return ret;
	}

	@Override
	public int indexOfKeyInMultiVal(K key, int rel) {
		int index = indexOfKeyInMultiVal(key);
		int insPoint = 0;
		if (index < 0)
			insPoint = -index - 1;

		switch (rel) {
		case Jiffy.LT:
			if (index >= 0)
				return index == 0 ? -1 : index - 1;
			return insPoint == 0 ? -1 : insPoint - 1;
		case Jiffy.LT | Jiffy.EQ:
			if (index >= 0)
				return index;
			return insPoint == 0 ? -1 : insPoint - 1;
		case Jiffy.GT | Jiffy.EQ:
			if (index >= 0)
				return index;
			return insPoint == size() ? -1 : insPoint;
		case Jiffy.GT:
			if (index >= 0)
				return index == size() - 1 ? -1 : index + 1;
			return insPoint == size() ? -1 : insPoint;
		default:
			throw new MultiversionNavigableMapException("Invalid argument.");
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public V get(K key) {
		if (FINGERPRINTS_STATISTICS) {
			fingerprintStats.get()[0]++;
		}

		if (USE_FINGERPRINTS) {
			int index = getIndexByHash(key);
			if (index == -1) {
				if (FINGERPRINTS_STATISTICS)
					fingerprintStats.get()[1]++;
				return null;
			} else if (index >= 0) {
				if (FINGERPRINTS_STATISTICS)
					fingerprintStats.get()[2]++;
				return (V) values[index];
			}
			if (FINGERPRINTS_STATISTICS)
				fingerprintStats.get()[3]++;
		}

		int index = indexOfKeyInMultiVal(key);

		if (index < 0)
			return null;
		else
			return (V) values[index];
	}

	@Override
	@SuppressWarnings("unchecked")
	public Map.Entry<K, V> getByIndex(int index) {
		if (index < 0 || index >= keys.length)
			return null;
		else
			return new AbstractMap.SimpleImmutableEntry<>((K) keys[index], (V) values[index]);
	}

	@Override
	@SuppressWarnings("unchecked")
	public K getKeyByIndex(Integer index) {
		return (K) keys[index];
	}

	@Override
	@SuppressWarnings("unchecked")
	public V getValueByIndex(int index) {
		return (V) values[index];
	}

	@Override
	public SingleMultiVal<K, V> add(K key, V value) {
		int index = indexOfKeyInMultiVal(key);
		if (index >= 0)
			return replaceValueAtPos(index, value);
		else
			return addAtPos(-index - 1, key, value);
	}

	@Override
	public SingleMultiVal<K, V> add(K key, V value, int index) {
		if (index >= 0)
			return replaceValueAtPos(index, value);
		else
			return addAtPos(-index - 1, key, value);
	}

	@Override
	public SingleMultiVal<K, V> remove(K key) {
		if (FINGERPRINTS_STATISTICS) {
			fingerprintStats.get()[3]++;
		}

		if (USE_FINGERPRINTS) {
			int index = getIndexByHash(key);
			if (index >= 0) {
				if (FINGERPRINTS_STATISTICS)
					fingerprintStats.get()[1]++;
				return removeItemAtPos(index);
			}

			return this;
		}

		int index = indexOfKeyInMultiVal(key);
		if (index >= 0)
			return removeItemAtPos(index);

		if (FINGERPRINTS_STATISTICS) {
			fingerprintStats.get()[4]++;
		}

		return this;
	}

	@Override
	public SingleMultiVal<K, V> remove(K key, int index) {
		if (index >= 0)
			return removeItemAtPos(index);
		return this;
	}

	private SingleMultiVal<K, V> replaceValueAtPos(int pos, V value) {
		if (keys.length == 0)
			throw new MultiversionNavigableMapException("MultiVal is too small.");

		SingleMultiVal<K, V> newMultiVal = new SingleMultiVal<K, V>(false);

		newMultiVal.keys = keys; 
		newMultiVal.values = Arrays.copyOf(values, values.length);
		newMultiVal.values[pos] = value;

		if (USE_FINGERPRINTS) {
			newMultiVal.hashes = hashes;
			newMultiVal.indices = indices;
		}

		return newMultiVal;
	}

	private SingleMultiVal<K, V> removeItemAtPos(int pos) {
		int oldLength = keys.length;
		if (oldLength == 1)
			return new SingleMultiVal<K, V>(true);

		SingleMultiVal<K, V> newMultiVal = new SingleMultiVal<K, V>(false);

		newMultiVal.keys = new Object[oldLength - 1];
		newMultiVal.values = new Object[oldLength - 1];

		System.arraycopy(keys, 0, newMultiVal.keys, 0, pos);
		System.arraycopy(keys, pos + 1, newMultiVal.keys, pos, keys.length - pos - 1);

		System.arraycopy(values, 0, newMultiVal.values, 0, pos);
		System.arraycopy(values, pos + 1, newMultiVal.values, pos, values.length - pos - 1);

		if (USE_FINGERPRINTS) {
			newMultiVal.hashes = new short[oldLength - 1];
			System.arraycopy(hashes, 0, newMultiVal.hashes, 0, pos);
			System.arraycopy(hashes, pos + 1, newMultiVal.hashes, pos, keys.length - pos - 1);

			newMultiVal.recalculateIndices();

			if (DEBUG)
				System.out.format("REMOVE:\n- old: %s\n- index: %d\n- new: %s\n", fpString(), pos,
						newMultiVal.fpString());
		}

		return newMultiVal;
	}

	private SingleMultiVal<K, V> addAtPos(int insertPos, K key, V val) {
		int oldLength = keys.length;
		if (oldLength == 0)
			return new SingleMultiVal<K, V>(this.comparator, key, val);

		SingleMultiVal<K, V> newMultiVal = new SingleMultiVal<K, V>(false);

		newMultiVal.keys = new Object[oldLength + 1];
		newMultiVal.values = new Object[oldLength + 1];

		System.arraycopy(keys, 0, newMultiVal.keys, 0, insertPos);
		newMultiVal.keys[insertPos] = key;
		System.arraycopy(keys, insertPos, newMultiVal.keys, insertPos + 1, keys.length - insertPos);

		System.arraycopy(values, 0, newMultiVal.values, 0, insertPos);
		newMultiVal.values[insertPos] = val;
		System.arraycopy(values, insertPos, newMultiVal.values, insertPos + 1, values.length - insertPos);

		if (USE_FINGERPRINTS) {
			newMultiVal.hashes = new short[oldLength + 1];
			System.arraycopy(hashes, 0, newMultiVal.hashes, 0, insertPos);
			newMultiVal.hashes[insertPos] = getHash(key);
			System.arraycopy(hashes, insertPos, newMultiVal.hashes, insertPos + 1, hashes.length - insertPos);

			newMultiVal.recalculateIndices();

			if (DEBUG)
				System.out.format("ADD:\n- old: %s\n- index: %d\n- new: %s\n", fpString(), insertPos,
						newMultiVal.fpString());

		}

		return newMultiVal;
	}

	public MultiVal<K, V> add(Batch<K, V> batch, MultiValIndices<K> indices) {
		if (indices.endSize == 0)
			return new SingleMultiVal<K, V>(true);

		SingleMultiVal<K, V> newMultiVal = new SingleMultiVal<K, V>(false);
		newMultiVal.keys = new Object[indices.endSize];
		newMultiVal.values = new Object[indices.endSize];
		if (USE_FINGERPRINTS)
			newMultiVal.hashes = new short[indices.endSize];

		int i = 0; // used on newMultiVal
		int lastIndex = 0;

		int firstKeyIndexInBatch = indices.indexOfFirstRelevantKeyInBatch;
		for (int j = 0; j < indices.indices.length; j++) {
			K key = batch.getKeyByIndex(firstKeyIndexInBatch + j);
			V value = batch.getValueByIndex(firstKeyIndexInBatch + j);

			int index = indices.indices[j];

			if (value == null && index < 0)
				continue;

			int absIndex = index >= 0 ? index : -(index + 1);
			int elementsToCopy = absIndex - lastIndex;

			if (elementsToCopy > 0) {
				System.arraycopy(keys, lastIndex, newMultiVal.keys, i, elementsToCopy);
				System.arraycopy(values, lastIndex, newMultiVal.values, i, elementsToCopy);
				if (USE_FINGERPRINTS)
					System.arraycopy(hashes, lastIndex, newMultiVal.hashes, i, elementsToCopy);
				i += elementsToCopy;
				lastIndex = absIndex;
			}

			if (value != null) {
				newMultiVal.keys[i] = key;
				newMultiVal.values[i] = value;
				if (USE_FINGERPRINTS)
					newMultiVal.hashes[i] = getHash(key);
				i++;
			}

			if (index >= 0)
				lastIndex++;
		}

		if (lastIndex < keys.length) {
			System.arraycopy(keys, lastIndex, newMultiVal.keys, i, keys.length - lastIndex);
			System.arraycopy(values, lastIndex, newMultiVal.values, i, keys.length - lastIndex);
			if (USE_FINGERPRINTS)
				System.arraycopy(hashes, lastIndex, newMultiVal.hashes, i, keys.length - lastIndex);
		}

		if (USE_FINGERPRINTS)
			newMultiVal.recalculateIndices();

		return newMultiVal;
	}

	@Override
	public Pair<MultiVal<K, V>, MultiVal<K, V>> addAndSplit(K key, V value, int index) {
		SingleMultiVal<K, V> leftMultiVal = new SingleMultiVal<K, V>(false);
		SingleMultiVal<K, V> rightMultiVal = new SingleMultiVal<K, V>(false);

		if (index < 0) {
			int insertPos = -index - 1;

			int leftLength = (keys.length + 1) / 2;
			int rightLength = keys.length + 1 - leftLength;

			if (insertPos >= leftLength) {
				leftMultiVal.keys = Arrays.copyOf(keys, leftLength);
				leftMultiVal.values = Arrays.copyOf(values, leftLength);

				rightMultiVal.keys = new Object[rightLength];
				rightMultiVal.values = new Object[rightLength];

				System.arraycopy(keys, leftLength, rightMultiVal.keys, 0, insertPos - leftLength);
				rightMultiVal.keys[insertPos - leftLength] = key;
				System.arraycopy(keys, insertPos, rightMultiVal.keys, insertPos - leftLength + 1,
						keys.length - insertPos);

				System.arraycopy(values, leftLength, rightMultiVal.values, 0, insertPos - leftLength);
				rightMultiVal.values[insertPos - leftLength] = value;
				System.arraycopy(values, insertPos, rightMultiVal.values, insertPos - leftLength + 1,
						values.length - insertPos);

				if (USE_FINGERPRINTS) {
					leftMultiVal.hashes = Arrays.copyOf(hashes, leftLength);
					rightMultiVal.hashes = new short[rightLength];
					System.arraycopy(hashes, leftLength, rightMultiVal.hashes, 0, insertPos - leftLength);
					rightMultiVal.hashes[insertPos - leftLength] = getHash(key);
					System.arraycopy(hashes, insertPos, rightMultiVal.hashes, insertPos - leftLength + 1,
							keys.length - insertPos);
				}
			} else {
				leftMultiVal.keys = new Object[leftLength];
				leftMultiVal.values = new Object[leftLength];

				rightMultiVal.keys = new Object[rightLength];
				rightMultiVal.values = new Object[rightLength];

				System.arraycopy(keys, 0, leftMultiVal.keys, 0, insertPos);
				leftMultiVal.keys[insertPos] = key;
				System.arraycopy(keys, insertPos, leftMultiVal.keys, insertPos + 1, leftLength - insertPos - 1);
				System.arraycopy(keys, leftLength - 1, rightMultiVal.keys, 0, rightLength);

				System.arraycopy(values, 0, leftMultiVal.values, 0, insertPos);
				leftMultiVal.values[insertPos] = value;
				System.arraycopy(values, insertPos, leftMultiVal.values, insertPos + 1, leftLength - insertPos - 1);
				System.arraycopy(values, leftLength - 1, rightMultiVal.values, 0, rightLength);

				if (USE_FINGERPRINTS) {
					leftMultiVal.hashes = new short[leftLength];
					rightMultiVal.hashes = new short[rightLength];

					System.arraycopy(hashes, 0, leftMultiVal.hashes, 0, insertPos);
					leftMultiVal.hashes[insertPos] = getHash(key);
					System.arraycopy(hashes, insertPos, leftMultiVal.hashes, insertPos + 1, leftLength - insertPos - 1);
					System.arraycopy(hashes, leftLength - 1, rightMultiVal.hashes, 0, rightLength);
				}
			}
		} else {
			int leftLength = keys.length / 2;
			int rightLength = keys.length - leftLength;

			leftMultiVal.keys = Arrays.copyOf(keys, leftLength);
			leftMultiVal.values = Arrays.copyOf(values, leftLength);

			rightMultiVal.keys = new Object[rightLength];
			rightMultiVal.values = new Object[rightLength];

			System.arraycopy(keys, leftLength, rightMultiVal.keys, 0, rightLength);
			System.arraycopy(values, leftLength, rightMultiVal.values, 0, rightLength);

			if (index < leftLength) {
				leftMultiVal.keys[index] = key;
				leftMultiVal.values[index] = value;
			} else {
				rightMultiVal.keys[index - leftLength] = key;
				rightMultiVal.values[index - leftLength] = value;
			}

			if (USE_FINGERPRINTS) {
				leftMultiVal.hashes = Arrays.copyOf(hashes, leftLength);
				rightMultiVal.hashes = new short[rightLength];
				System.arraycopy(hashes, leftLength, rightMultiVal.hashes, 0, rightLength);

				if (index < leftLength) {
					leftMultiVal.hashes[index] = getHash(key);
				} else {
					rightMultiVal.hashes[index - leftLength] = getHash(key);
				}
			}
		}

		if (USE_FINGERPRINTS) {
			leftMultiVal.recalculateIndices();
			rightMultiVal.recalculateIndices();

			if (DEBUG)
				System.out.format("SPLIT:\n- old: %s\n- index: %d\n- left: %s\n- right: %s\n", fpString(), index,
						leftMultiVal.fpString(), rightMultiVal.fpString());
		}

		return new Pair<>(leftMultiVal, rightMultiVal);
	}

	private void recalculateIndices() {
		indices = new short[2 * keys.length];

		for (int i = 0; i < keys.length; i++) {
			short shortHash = getMicroHash(hashes[i]);
			indices[2 * shortHash + 1] = indices[2 * shortHash];
			indices[2 * shortHash] = (short) (i + 1);
		}
	}

	private String fpString() {
		StringBuilder builder = new StringBuilder();
		builder.append(Arrays.toString(keys) + " " + Arrays.toString(values) + " " + Arrays.toString(hashes) + " "
				+ Arrays.toString(indices));
		return builder.toString();
	}

	public Pair<MultiVal<K, V>, MultiVal<K, V>> addAndSplit(Batch<K, V> batch, MultiValIndices<K> indices) {
		SingleMultiVal<K, V> leftMultiVal = new SingleMultiVal<K, V>(false);
		int leftMultiValSize = indices.endSize / 2;
		leftMultiVal.keys = new Object[leftMultiValSize];
		leftMultiVal.values = new Object[leftMultiValSize];
		if (USE_FINGERPRINTS)
			leftMultiVal.hashes = new short[leftMultiValSize];

		SingleMultiVal<K, V> rightMultiVal = new SingleMultiVal<K, V>(false);
		int rightMultiValSize = indices.endSize - leftMultiValSize;
		rightMultiVal.keys = new Object[rightMultiValSize];
		rightMultiVal.values = new Object[rightMultiValSize];
		if (USE_FINGERPRINTS)
			rightMultiVal.hashes = new short[rightMultiValSize];

		int i = 0; // used on newMultiVal
		SingleMultiVal<K, V> currentMultiVal = leftMultiVal;
		int currMultiValSize = leftMultiValSize;
		int lastIndex = 0;

		int firstKeyIndexInBatch = indices.indexOfFirstRelevantKeyInBatch;
		for (int j = 0; j < indices.indices.length; j++) {
			K key = batch.getKeyByIndex(firstKeyIndexInBatch + j);
			V value = batch.getValueByIndex(firstKeyIndexInBatch + j);

			int index = indices.indices[j];

			if (value == null && index < 0)
				continue;

			int absIndex = index >= 0 ? index : -(index + 1);
			int elementsToCopy = absIndex - lastIndex;

			if (elementsToCopy > 0) {
				if (i + elementsToCopy < currMultiValSize) {
					System.arraycopy(keys, lastIndex, currentMultiVal.keys, i, elementsToCopy);
					System.arraycopy(values, lastIndex, currentMultiVal.values, i, elementsToCopy);
					if (USE_FINGERPRINTS)
						System.arraycopy(hashes, lastIndex, currentMultiVal.hashes, i, elementsToCopy);
					i += elementsToCopy;
					lastIndex = absIndex;

					if (i == currMultiValSize) {
						currentMultiVal = rightMultiVal;
						currMultiValSize = rightMultiValSize;
						i = 0;
					}
				} else {
					int elementsToCopyFirst = currMultiValSize - i;

					System.arraycopy(keys, lastIndex, currentMultiVal.keys, i, elementsToCopyFirst);
					System.arraycopy(values, lastIndex, currentMultiVal.values, i, elementsToCopyFirst);
					if (USE_FINGERPRINTS)
						System.arraycopy(hashes, lastIndex, currentMultiVal.hashes, i, elementsToCopyFirst);

					lastIndex += elementsToCopyFirst;
					elementsToCopy -= elementsToCopyFirst;
					currentMultiVal = rightMultiVal;
					currMultiValSize = rightMultiValSize;
					i = 0;

					System.arraycopy(keys, lastIndex, currentMultiVal.keys, i, elementsToCopy);
					System.arraycopy(values, lastIndex, currentMultiVal.values, i, elementsToCopy);
					if (USE_FINGERPRINTS)
						System.arraycopy(hashes, lastIndex, currentMultiVal.hashes, i, elementsToCopy);

					i += elementsToCopy;
				}
				lastIndex = absIndex;
			}

			if (value != null) {
				currentMultiVal.keys[i] = key;
				currentMultiVal.values[i] = value;
				if (USE_FINGERPRINTS)
					currentMultiVal.hashes[i] = getHash(key);
				i++;

				if (i == currMultiValSize) {
					currentMultiVal = rightMultiVal;
					currMultiValSize = rightMultiValSize;
					i = 0;
				}
			}

			if (index >= 0)
				lastIndex++;
		}

		if (lastIndex < keys.length) {
			int elementsToCopy = keys.length - lastIndex;

			if (elementsToCopy > 0) {
				if (i + elementsToCopy < currMultiValSize) {
					System.arraycopy(keys, lastIndex, currentMultiVal.keys, i, elementsToCopy);
					System.arraycopy(values, lastIndex, currentMultiVal.values, i, elementsToCopy);
					if (USE_FINGERPRINTS)
						System.arraycopy(hashes, lastIndex, currentMultiVal.hashes, i, elementsToCopy);
				} else {
					int elementsToCopyFirst = currMultiValSize - i;

					System.arraycopy(keys, lastIndex, currentMultiVal.keys, i, elementsToCopyFirst);
					System.arraycopy(values, lastIndex, currentMultiVal.values, i, elementsToCopyFirst);
					if (USE_FINGERPRINTS)
						System.arraycopy(hashes, lastIndex, currentMultiVal.hashes, i, elementsToCopyFirst);
					lastIndex += elementsToCopyFirst;

					elementsToCopy -= elementsToCopyFirst;
					currentMultiVal = rightMultiVal;
					currMultiValSize = rightMultiValSize;

					i = 0;
					System.arraycopy(keys, lastIndex, currentMultiVal.keys, i, elementsToCopy);
					System.arraycopy(values, lastIndex, currentMultiVal.values, i, elementsToCopy);
					if (USE_FINGERPRINTS)
						System.arraycopy(hashes, lastIndex, currentMultiVal.hashes, i, elementsToCopy);
				}
			}
		}

		if (USE_FINGERPRINTS) {
			leftMultiVal.recalculateIndices();
			rightMultiVal.recalculateIndices();
		}

		return new Pair<>(leftMultiVal, rightMultiVal);
	}

	private void mergeAndRemove(MultiVal<K, V> mvalLeft, MultiVal<K, V> mvalRight, int indexOfKeyInNextMultiVal) {
		SingleMultiVal<K, V> sMvalLeft = (SingleMultiVal<K, V>) mvalLeft;
		SingleMultiVal<K, V> sMvalRight = (SingleMultiVal<K, V>) mvalRight;

		int newSize = sMvalLeft.size() + sMvalRight.size() - 1;
		keys = new Object[newSize];
		values = new Object[newSize];

		if (sMvalLeft.keys != null) {
			System.arraycopy(sMvalLeft.keys, 0, keys, 0, sMvalLeft.keys.length);
			System.arraycopy(sMvalLeft.values, 0, values, 0, sMvalLeft.values.length);
		}

		int index = sMvalLeft.keys.length;
		if (sMvalRight.keys != null) {
			System.arraycopy(sMvalRight.keys, 0, keys, index, indexOfKeyInNextMultiVal);
			System.arraycopy(sMvalRight.values, 0, values, index, indexOfKeyInNextMultiVal);
		}

		System.arraycopy(sMvalRight.keys, indexOfKeyInNextMultiVal + 1, keys, index + indexOfKeyInNextMultiVal,
				sMvalRight.keys.length - indexOfKeyInNextMultiVal - 1);
		System.arraycopy(sMvalRight.values, indexOfKeyInNextMultiVal + 1, values, index + indexOfKeyInNextMultiVal,
				sMvalRight.values.length - indexOfKeyInNextMultiVal - 1);

		if (USE_FINGERPRINTS) {
			hashes = new short[newSize];

			if (sMvalLeft.keys != null)
				System.arraycopy(sMvalLeft.hashes, 0, hashes, 0, sMvalLeft.keys.length);
			if (sMvalRight.keys != null)
				System.arraycopy(sMvalRight.hashes, 0, hashes, index, indexOfKeyInNextMultiVal);
			System.arraycopy(sMvalRight.hashes, indexOfKeyInNextMultiVal + 1, hashes, index + indexOfKeyInNextMultiVal,
					sMvalRight.keys.length - indexOfKeyInNextMultiVal - 1);

			recalculateIndices();

			if (DEBUG)
				System.out.format("MERGE:\n- left: %s\n- right: %s\n- index: %d\n- new: %s\n", sMvalLeft.fpString(),
						sMvalRight.fpString(), indexOfKeyInNextMultiVal, fpString());
		}
	}

	private void mergeAndAdd(Comparator<? super K> comparator, Batch<K, V> batch, MultiVal<K, V> leftMval,
			MultiVal<K, V> rightMval, MultiValIndices<K> leftIndices, MultiValIndices<K> rightIndices) {
		int totalSize = leftIndices.endSize + rightIndices.endSize;

		keys = new Object[totalSize];
		values = new Object[totalSize];

		if (USE_FINGERPRINTS)
			hashes = new short[totalSize];

		int i = 0; // used on newMultiVal, i.e., this

		// copying from leftMval

		int lastIndex = 0;
		Object[] currentKeys = leftMval.getKeys();
		Object[] currentValues = leftMval.getValues();
		short[] currentHashes = null;
		if (USE_FINGERPRINTS)
			currentHashes = ((SingleMultiVal<K, V>) leftMval).hashes;

		int firstKeyIndexInBatch = leftIndices.indices.length > 0 ? leftIndices.indexOfFirstRelevantKeyInBatch : 0;
		for (int j = 0; j < leftIndices.indices.length; j++) {
			K key = batch.getKeyByIndex(firstKeyIndexInBatch + j);
			V value = batch.getValueByIndex(firstKeyIndexInBatch + j);

			int index = leftIndices.indices[j];

			if (value == null && index < 0)
				continue;

			int absIndex = index >= 0 ? index : -(index + 1);
			int elementsToCopy = absIndex - lastIndex;

			if (elementsToCopy > 0) {
				System.arraycopy(currentKeys, lastIndex, keys, i, elementsToCopy);
				System.arraycopy(currentValues, lastIndex, values, i, elementsToCopy);
				if (USE_FINGERPRINTS)
					System.arraycopy(currentHashes, lastIndex, hashes, i, elementsToCopy);
				i += elementsToCopy;
				lastIndex = absIndex;
			}

			if (value != null) {
				keys[i] = key;
				values[i] = value;
				if (USE_FINGERPRINTS)
					hashes[i] = getHash(key);
				i++;
			}

			if (index >= 0)
				lastIndex++;
		}

		if (lastIndex < currentKeys.length) {
			int elementsToCopy = currentKeys.length - lastIndex;
			System.arraycopy(currentKeys, lastIndex, keys, i, elementsToCopy);
			System.arraycopy(currentValues, lastIndex, values, i, elementsToCopy);
			if (USE_FINGERPRINTS)
				System.arraycopy(currentHashes, lastIndex, hashes, i, elementsToCopy);
			i += elementsToCopy;
		}

		// copying from rightMval

		lastIndex = 0;
		currentKeys = rightMval.getKeys();
		currentValues = rightMval.getValues();
		if (USE_FINGERPRINTS)
			currentHashes = ((SingleMultiVal<K, V>) rightMval).hashes;

		firstKeyIndexInBatch = rightIndices.indices.length > 0 ? rightIndices.indexOfFirstRelevantKeyInBatch : 0;
		for (int j = 0; j < rightIndices.indices.length; j++) {
			K key = batch.getKeyByIndex(firstKeyIndexInBatch + j);
			V value = batch.getValueByIndex(firstKeyIndexInBatch + j);

			int index = rightIndices.indices[j];

			if (value == null && index < 0)
				continue;

			int absIndex = index >= 0 ? index : -(index + 1);
			int elementsToCopy = absIndex - lastIndex;

			if (elementsToCopy > 0) {
				System.arraycopy(currentKeys, lastIndex, keys, i, elementsToCopy);
				System.arraycopy(currentValues, lastIndex, values, i, elementsToCopy);
				if (USE_FINGERPRINTS)
					System.arraycopy(currentHashes, lastIndex, hashes, i, elementsToCopy);
				i += elementsToCopy;
				lastIndex = absIndex;
			}

			if (value != null) {
				keys[i] = key;
				values[i] = value;
				hashes[i] = getHash(key);
				i++;
			}

			if (index >= 0)
				lastIndex++;
		}

		if (lastIndex < currentKeys.length) {
			int elementsToCopy = currentKeys.length - lastIndex;
			System.arraycopy(currentKeys, lastIndex, keys, i, elementsToCopy);
			System.arraycopy(currentValues, lastIndex, values, i, elementsToCopy);
			if (USE_FINGERPRINTS)
				System.arraycopy(currentHashes, lastIndex, hashes, i, elementsToCopy);
		}

		if (USE_FINGERPRINTS)
			recalculateIndices();
	}

	@Override
	public Iterator<Map.Entry<K, V>> iterator() {
		return iterator(0);
	}

	@Override
	public Iterator<Map.Entry<K, V>> iterator(int index) {
		return new AscendingKeyValueIterator(index);
	}

	@Override
	public Iterator<Map.Entry<K, V>> descendingIterator() {
		return new DescendingKeyValueIterator(keys.length == 0 ? 0 : keys.length - 1);
	}

	@Override
	public Iterator<Map.Entry<K, V>> descendingIterator(int index) {
		return new DescendingKeyValueIterator(index);
	}

	@Override
	public String toString() {
		return "[" + (keys == null ? "null" : Arrays.toString(keys)) + " "
				+ (keys == null ? "null" : Arrays.toString(values)) + "]";
	}

	abstract class KeyValueIterator implements Iterator<Map.Entry<K, V>> {
		int index = 0;

		public KeyValueIterator(int index) {
			this.index = index;
		}

		public abstract Map.Entry<K, V> next();
	}

	final class AscendingKeyValueIterator extends KeyValueIterator {
		public AscendingKeyValueIterator(int index) {
			super(index);
		}

		@Override
		public boolean hasNext() {
			return index < keys.length;
		}

		@SuppressWarnings("unchecked")

		@Override
		public Map.Entry<K, V> next() {
			if (index >= keys.length)
				throw new NoSuchElementException();

			var entry = new AbstractMap.SimpleImmutableEntry<K, V>((K) keys[index], (V) values[index]);
			index++;
			return entry;
		}
	}

	final class DescendingKeyValueIterator extends KeyValueIterator {
		public DescendingKeyValueIterator(int index) {
			super(index);
		}

		@Override
		public boolean hasNext() {
			return index < keys.length && index >= 0;
		}

		@SuppressWarnings("unchecked")
		@Override
		public Map.Entry<K, V> next() {
			if (index >= keys.length || index < 0)
				throw new NoSuchElementException();

			var entry = new AbstractMap.SimpleImmutableEntry<K, V>((K) keys[index], (V) values[index]);
			index--;
			return entry;
		}
	}
}
