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

import java.util.Iterator;
import java.util.Map;

import pl.edu.put.concurrent.jiffy.SingleMultiVal.MultiValIndices;
import pl.edu.put.utils.Pair;

interface MultiVal<K, V> {

	SingleMultiVal<K, V> clone();

	K firstKey();

	K lastKey();

	// Based on open JDK 8 code Arrays.binarySearch
	int indexOfKeyInMultiVal(K key);
	
	int indexOfKeyInMultiVal(K key, boolean fastPath);

	int indexOfKeyInMultiVal(K key, int rel);

	V get(K key);

	Map.Entry<K, V> getByIndex(int index);

	K getKeyByIndex(Integer index);
	
	V getValueByIndex(int index);

	SingleMultiVal<K, V> add(K key, V value);

	SingleMultiVal<K, V> add(K key, V value, int index);

	SingleMultiVal<K, V> remove(K key);

	SingleMultiVal<K, V> remove(K key, int index);

	int size();

	boolean containsKey(K key);

	Object[] getKeys();

	Object[] getValues();

	V firstValue();

	V lastValue();

	boolean containsValue(V value);

	Pair<MultiVal<K, V>, MultiVal<K, V>> addAndSplit(K key, V value, int index);

	Iterator<Map.Entry<K, V>> iterator();
	
	Iterator<Map.Entry<K, V>> iterator(int index);
	
	Iterator<Map.Entry<K, V>> descendingIterator();
	
	Iterator<Map.Entry<K, V>> descendingIterator(int index);
	
	String toString();

	MultiValIndices<K> indexOfKeysInMultiVal(Batch<K, V> batch, int indexOfFirstRelevantBatchKeyFromRight, K nodeKey);
	
	MultiVal<K, V> add(Batch<K, V> batch, MultiValIndices<K> indices);

	Pair<MultiVal<K, V>, MultiVal<K, V>> addAndSplit(Batch<K, V> batch, MultiValIndices<K> indices);
}