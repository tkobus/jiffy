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

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class Batch<K, V> {
	private Map<K, V> map = new LinkedHashMap<>();
	private HashMap<K, V> substitutedValuesMap = new HashMap<>();
	
	private K[] keys;
	private V[] values;

	public Batch() {
	}

	public Batch(Map<K, V> map) {
		put(map);
	}

	public Map<K, V> getMap() {
		return map;
	}
	
	public V put(K key, V value) {
		return map.put(key, value);
	}
	
	public V remove(K key) {
		return map.put(key, null);
	}

	public void put(Map<K, V> map) {
		this.map.putAll(map);
	}

	public void setSubstitutedValue(K key, V value) {
		substitutedValuesMap.put(key, value);
	}

	public Map<K, V> getSubstitutedValues() {
		return substitutedValuesMap;
	}

	public String toString() {
//		return "Batch: " + map.toString() + " " + Arrays.toString(keys) + " " + Arrays.toString(values);
		return String.format("[Batch: %s, keys: %s, values: %s]", map, Arrays.toString(keys), Arrays.toString(values));
	}

	public int mapSize() {
		return map.size();
	}

	public Map.Entry<K, V> mapOnlyEntry() {
		assert map.size() == 1;
		return map.entrySet().iterator().next();
	}

	public int size() {
		return keys.length;
	}

	public K lastKey() {
		assert keys.length > 0;

		return keys[keys.length - 1];
	}

	public K lowerKey(K key) {
		int index = Arrays.binarySearch(keys, key);

		int insPoint = index >= 0 ? index : -index - 1;
		if (insPoint > 0)
			return keys[insPoint - 1];
		return null;
	}
	
//	public int indexOfLowerKey(K key) {
//		int index = Arrays.binarySearch(keys, key);
//
//		int insPoint = index >= 0 ? index : -index - 1;
//		if (insPoint > 0)
//			return insPoint - 1;
//		return -1;
//	}

	@SuppressWarnings("unchecked")
	public void prepare() {
		keys = (K[]) map.keySet().toArray();
		Arrays.sort(keys);
		values = (V[]) (new Object[keys.length]);
		for (int i = 0; i < keys.length; i++) {
			K k = keys[i];
			values[i] = map.get(k);
		}
	}

	public int getKeyIndex(K key) {
		return Arrays.binarySearch(keys, key);
	}

	public K getKeyByIndex(int index) {
		return keys[index];
	}

	public V getValueByIndex(int index) {
		return values[index];
	}

	public int ceilingKeyIndex(K key) {
		int index = Arrays.binarySearch(keys, key);
		if (index >= 0)
			return index;
		return -index - 1;
	}
	
	// For tests only, we want to map to be const.
	public Batch(HashMap<K, V> map) {
		this.map = map;
	}
	
	// For debug only.
	public K[] getKeys() {
		return keys;
	}
	
	public V[] getValues() {
		return values;
	}
}
