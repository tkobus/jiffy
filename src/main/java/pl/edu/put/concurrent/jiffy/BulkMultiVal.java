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

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import pl.edu.put.concurrent.MultiversionNavigableMapException;
import pl.edu.put.utils.Pair;

class BulkMultiVal<K, V> extends SingleMultiVal<K, V> {
	Comparator<? super K> comparator;

	@SuppressWarnings("rawtypes")
	MultiVal[] multiVals;
	int[] multiValsCumulSizes;
	int totalSize;

	public static <K, V> MultiVal<K, V> createBulkRevision(Comparator<? super K> comparator,
			List<Revision<K, V>> sortedRevisions) {
		int sortedRevisionsSize = sortedRevisions.size();
		if (sortedRevisionsSize == 0)
			return new SingleMultiVal<>(true); // does this happen?
		else if (sortedRevisionsSize == 1)
			return sortedRevisions.get(0).getValue();
		return new BulkMultiVal<>(comparator, sortedRevisions);
	}

	public BulkMultiVal(Comparator<? super K> comparator, List<Revision<K, V>> sortedRevisions) {
		super(false);
		this.comparator = comparator;

		int size = sortedRevisions.size();
		multiVals = new MultiVal[size];
		multiValsCumulSizes = new int[size];
		totalSize = 0;
		for (int i = 0; i < size; i++) {
			multiVals[i] = sortedRevisions.get(i).getValue();
			multiValsCumulSizes[i] = totalSize;
			totalSize += multiVals[i].size();
		}
	}

	public BulkMultiVal<K, V> clone() {
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("unchecked")
	public K firstKey() {
		return (K) multiVals[0].firstKey();
	}

	@SuppressWarnings("unchecked")
	public K lastKey() {
		return (K) multiVals[multiVals.length - 1].lastKey();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	static int cpr(Comparator c, Object x, Object y) {
		return (c != null) ? c.compare(x, y) : ((Comparable) x).compareTo(y);
	}

	// Temporarily for tests
	public int findShard(K key) {
		int low = 0;
		int high = multiVals.length - 1;
		
		int ret = 0;
		while (low <= high) {
			int mid = (low + high) >>> 1;
			Object midVal = multiVals[mid].firstKey();
			@SuppressWarnings("unchecked")
			int cmp = cpr(comparator, key, (K) midVal);
			if (cmp == 0)
				return mid;
			else if (cmp > 0) {
				low = mid + 1;
				ret = mid;
			} else if (cmp < 0)
				high = mid - 1;
		}

		return ret;
	}

	private int findShardByIndex(int index) {
		int insPoint = index >= 0 ? index : -index - 1;

		int low = 0;
		int high = multiVals.length - 1;

		int ret = 0;
		while (low <= high) {
			int mid = (low + high) >>> 1;
			int midVal = multiValsCumulSizes[mid];
			int cmp = insPoint - midVal;
			if (cmp == 0)
				return mid;
			else if (cmp > 0) {
				low = mid + 1;
				ret = mid;
			} else if (cmp < 0)
				high = mid - 1;
		}

		return ret;
	}

	public int indexOfKeyInMultiVal(K key) {
		int shard = findShard(key);
		@SuppressWarnings("unchecked")
		int indexInShard = multiVals[shard].indexOfKeyInMultiVal(key);
		if (indexInShard >= 0)
			return multiValsCumulSizes[shard] + indexInShard;
		else
			return -multiValsCumulSizes[shard] + indexInShard;
	}

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

	public V get(K key) {
		int index = indexOfKeyInMultiVal(key);

		if (index < 0) {
			return null;
		} else {
			return getByIndex(index).getValue();
		}
	}

	@SuppressWarnings("unchecked")
	public Map.Entry<K, V> getByIndex(int index) {
		int shardIndex = findShardByIndex(index);
		int trueIndex = index - multiValsCumulSizes[shardIndex];
		MultiVal<K, V> shard = multiVals[shardIndex];
		return shard.getByIndex(trueIndex);
	}

	@SuppressWarnings("unchecked")
	public K getKeyByIndex(Integer index) {
		int shardIndex = findShardByIndex(index);
		int trueIndex = index - multiValsCumulSizes[shardIndex];
		MultiVal<K, V> shard = multiVals[shardIndex];
		return shard.getKeyByIndex(trueIndex);
	}

	public BulkMultiVal<K, V> add(K key, V value) {
		throw new UnsupportedOperationException();
	}

	public BulkMultiVal<K, V> add(K key, V value, int index) {
		throw new UnsupportedOperationException();
	}

	public BulkMultiVal<K, V> remove(K key) {
		throw new UnsupportedOperationException();
	}

	public BulkMultiVal<K, V> remove(K key, int index) {
		throw new UnsupportedOperationException();
	}

	public int size() {
		return totalSize;
	}

	public boolean containsKey(K key) {
		return indexOfKeyInMultiVal(key) >= 0;
	}

	@SuppressWarnings("unchecked")
	public Object[] getKeys() {
		// only for tests
		Object[] ret = new Object[totalSize];
		int i = 0;
		for (MultiVal<K, V> mv : multiVals) {
			Object[] keys = mv.getKeys();
			for (int j = 0; j < keys.length; i++, j++)
				ret[i] = keys[j];
		}
		return ret;
	}

	@SuppressWarnings("unchecked")
	public Object[] getValues() {
		// only for tests
		Object[] ret = new Object[totalSize];
		int i = 0;
		for (MultiVal<K, V> mv : multiVals) {
			Object[] values = mv.getValues();
			for (int j = 0; j < values.length; i++, j++)
				ret[i] = values[j];
		}
		return ret;
	}

	@SuppressWarnings("unchecked")
	public V firstValue() {
		return size() == 0 ? null : (V) multiVals[0].firstValue();
	}

	@SuppressWarnings("unchecked")
	public V lastValue() {
		return size() == 0 ? null : (V) multiVals[multiVals.length - 1].lastValue();
	}

	@SuppressWarnings("unchecked")
	public boolean containsValue(V value) {
		for (MultiVal<K, V> mv : multiVals) {
			if (mv.containsValue(value))
				return true;
		}

		return false;
	}

	public Pair<MultiVal<K, V>, MultiVal<K, V>> addAndSplit(K key, V value, int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<Map.Entry<K, V>> iterator() {
		return new KeyValueIterator(false);
	}

	@Override
	public Iterator<Map.Entry<K, V>> iterator(int index) {
		return new KeyValueIterator(index, false);
	}

	@Override
	public Iterator<Map.Entry<K, V>> descendingIterator() {
		return new KeyValueIterator(true);
	}

	@Override
	public Iterator<Map.Entry<K, V>> descendingIterator(int index) {
		return new KeyValueIterator(index, true);
	}

	@SuppressWarnings("unchecked")
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();

		builder.append("[");
		for (MultiVal<K, V> mv : multiVals)
			builder.append(mv.toString()).append(", ");

		builder.replace(builder.length() - 2, builder.length(), "");
		builder.append("]");

		return builder.toString();
	}

	final class KeyValueIterator implements Iterator<Map.Entry<K, V>> {
		boolean descending;
		Integer index = 0;
		int currentShardIndex = 0;
		Iterator<Map.Entry<K, V>> currentIterator;

		@SuppressWarnings("unchecked")
		public KeyValueIterator(boolean descending) {
			this.descending = descending;
			if (descending) {
				currentShardIndex = multiVals.length - 1;
				currentIterator = multiVals[currentShardIndex].descendingIterator();
			} else
				currentIterator = multiVals[currentShardIndex].iterator();

			advance();
		}

		@SuppressWarnings("unchecked")
		public KeyValueIterator(int index, boolean descending) {
			this.index = index;
			this.descending = descending;
			currentShardIndex = findShardByIndex(index);
			int trueIndex = index - multiValsCumulSizes[currentShardIndex];
			if (descending)
				currentIterator = multiVals[currentShardIndex].descendingIterator(trueIndex);
			else
				currentIterator = multiVals[currentShardIndex].iterator(trueIndex);

			advance();
		}

		@SuppressWarnings("unchecked")
		protected void advance() {
			while (!currentIterator.hasNext()) {
				if (descending) {
					currentShardIndex--;
					if (currentShardIndex < 0)
						break;
					currentIterator = multiVals[currentShardIndex].descendingIterator();
				} else {
					currentShardIndex++;
					if (currentShardIndex == multiVals.length)
						break;
					currentIterator = multiVals[currentShardIndex].iterator();
				}
			}
		}

		@Override
		public boolean hasNext() {
			return currentIterator.hasNext();
		}

		@Override
		public Map.Entry<K, V> next() {
			var ret = currentIterator.next();
			advance();
			return ret;
		}
	}
}
