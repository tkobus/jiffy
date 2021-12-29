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

package pl.edu.put.concurrent.conctest;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.jgrapht.Graph;

public class ProgramOrderOperations {

	// threadId -> timestamp -> seq -> node
	Map<Integer, NavigableMap<Long, NavigableMap<Integer, Node>>> opMinTimestampMap;
	// threadId -> timestamp -> seq -> node
	Map<Integer, NavigableMap<Long, NavigableMap<Integer, Node>>> opMaxTimestampMap;
	// threadId -> key -> timestamp -> seq -> node
	Map<Integer, Map<Integer, NavigableMap<Long, NavigableMap<Integer, Node>>>> updatesMap;

	public ProgramOrderOperations(Graph<Node, LabeledEdge> graph) {
		opMinTimestampMap = new HashMap<>();
		opMaxTimestampMap = new HashMap<>();
		updatesMap = new HashMap<>();

		init(graph);
	}

	public Set<Integer> getThreadIds() {
		return opMinTimestampMap.keySet();
	}

	private void init(Graph<Node, LabeledEdge> graph) {
		Node root = null;
		
		for (Node n : graph.vertexSet()) {
			if (n.event == null) {
				root = n;
				continue;
			}

			addOpMinTimestampEntry(n);
			addOpMaxTimestampEntry(n);
			if (n.isUpdating())
				addUpdateEntry(n);
		}
		
		addRootUpdateEntry(root);
	}
	
	private void addOpMinTimestampEntry(Node n) {
		int threadId = n.getThreadId();
		assert threadId != -1;

		long timestamp = n.minTimestamp;
		int seq = n.getSeqNum();

		NavigableMap<Long, NavigableMap<Integer, Node>> threadTimestampMap = opMinTimestampMap.get(threadId);
		if (threadTimestampMap == null) {
			threadTimestampMap = new TreeMap<>();
			opMinTimestampMap.put(threadId, threadTimestampMap);
		}

		NavigableMap<Integer, Node> threadTimestampSeqMap = threadTimestampMap.get(timestamp);
		if (threadTimestampSeqMap == null) {
			threadTimestampSeqMap = new TreeMap<>();
			threadTimestampMap.put(timestamp, threadTimestampSeqMap);
		}

		threadTimestampSeqMap.put(seq, n);
	}
	
	private void addOpMaxTimestampEntry(Node n) {
		int threadId = n.getThreadId();
		assert threadId != -1;
		
		long timestamp = n.maxTimestamp;
		int seq = n.getSeqNum();

		NavigableMap<Long, NavigableMap<Integer, Node>> threadTimestampMap = opMaxTimestampMap.get(threadId);
		if (threadTimestampMap == null) {
			threadTimestampMap = new TreeMap<>();
			opMaxTimestampMap.put(threadId, threadTimestampMap);
		}

		NavigableMap<Integer, Node> threadTimestampSeqMap = threadTimestampMap.get(timestamp);
		if (threadTimestampSeqMap == null) {
			threadTimestampSeqMap = new TreeMap<>();
			threadTimestampMap.put(timestamp, threadTimestampSeqMap);
		}

		threadTimestampSeqMap.put(seq, n);
	}

	private void addRootUpdateEntry(Node n) {
		for (Integer t : getThreadIds()) {
			Map<Integer, NavigableMap<Long, NavigableMap<Integer, Node>>> threadKeyMap = updatesMap.get(t);
			if (threadKeyMap == null)
				continue;
			for (var e : threadKeyMap.entrySet()) {
				// int key = e.getKey();
				NavigableMap<Long, NavigableMap<Integer, Node>> threadKeyTimestampMap = e.getValue();
				
				NavigableMap<Integer, Node> threadKeyTimestampSeqMap = new TreeMap<>();
				threadKeyTimestampSeqMap.put(-1, n);
				threadKeyTimestampMap.put(0l, threadKeyTimestampSeqMap);
			}
		}
	}
	
	private void addUpdateEntry(Node n) {
		assert !n.isUpdating();

		int threadId = n.getThreadId();
		assert threadId != -1;

		long timestamp = n.minTimestamp;
		int seq = n.getSeqNum();
		int[] keys = n.getUpdatedKeys();

		for (int key : keys) {
			Map<Integer, NavigableMap<Long, NavigableMap<Integer, Node>>> threadKeyMap = updatesMap.get(threadId);
			if (threadKeyMap == null) {
				threadKeyMap = new HashMap<>();
				updatesMap.put(threadId, threadKeyMap);
			}

			NavigableMap<Long, NavigableMap<Integer, Node>> threadKeyTimestampMap = threadKeyMap.get(key);
			if (threadKeyTimestampMap == null) {
				threadKeyTimestampMap = new TreeMap<>();
				threadKeyMap.put(key, threadKeyTimestampMap);
			}

			NavigableMap<Integer, Node> threadKeyTimestampSeqMap = threadKeyTimestampMap.get(timestamp);
			if (threadKeyTimestampSeqMap == null) {
				threadKeyTimestampSeqMap = new TreeMap<>();
				threadKeyTimestampMap.put(timestamp, threadKeyTimestampSeqMap);
			}

			threadKeyTimestampSeqMap.put(seq, n);
		}
	}

	private void removeOpMinTimestampEntry(Node n) {
		int threadId = n.getThreadId();
		if (threadId == -1)
			return;

		long timestamp = n.minTimestamp;
		int seq = n.getSeqNum();

		NavigableMap<Integer, Node> threadTimestampSeqMap = opMinTimestampMap.get(threadId).get(timestamp);
		threadTimestampSeqMap.remove(seq);
		if (threadTimestampSeqMap.size() == 0)
			opMinTimestampMap.get(threadId).remove(timestamp);
	}

	private void removeOpMaxTimestampEntry(Node n) {
		int threadId = n.getThreadId();
		if (threadId == -1)
			return;

		long timestamp = n.maxTimestamp;
		int seq = n.getSeqNum();

		NavigableMap<Integer, Node> threadTimestampSeqMap = opMaxTimestampMap.get(threadId).get(timestamp);
		threadTimestampSeqMap.remove(seq);
		if (threadTimestampSeqMap.size() == 0)
			opMaxTimestampMap.get(threadId).remove(timestamp);
	}

	private void removeUpdateEntry(Node n) {
		int threadId = n.getThreadId();
		if (threadId == -1 || !n.isUpdating())
			return;

		long timestamp = n.minTimestamp;
		int seq = n.getSeqNum();

		int[] keys = n.getUpdatedKeys();

		for (int key : keys) {
			NavigableMap<Long, NavigableMap<Integer, Node>> threadKeyTimestampMap = updatesMap.get(threadId).get(key);
			NavigableMap<Integer, Node> threadKeyTimestampSeqMap = threadKeyTimestampMap.get(timestamp);
			threadKeyTimestampSeqMap.remove(seq);
			if (threadKeyTimestampSeqMap.size() == 0)
				updatesMap.get(threadId).get(key).remove(timestamp);
			if (threadKeyTimestampMap.size() == 0)
				updatesMap.get(threadId).remove(key);
		}
	}

	public Node updateMinTimestamp(Node n, long newTimestamp) {
		int threadId = n.getThreadId();
		if (threadId == -1)
			return null;

		boolean isUpdating = n.isUpdating();

		removeOpMinTimestampEntry(n);
		if (isUpdating)
			removeUpdateEntry(n);

		n.minTimestamp = newTimestamp;

		addOpMinTimestampEntry(n);
		if (isUpdating)
			addUpdateEntry(n);

		return n;
	}

	public Node updateMaxTimestamp(Node n, long newTimestamp) {
		int threadId = n.getThreadId();
		if (threadId == -1)
			return null;

		removeOpMaxTimestampEntry(n);

		n.maxTimestamp = newTimestamp;

		addOpMaxTimestampEntry(n);

		return n;
	}

	// nodes just after n, in real-time, on other threads
	public Set<Node> getNodesJustAfter(Node n) {
		long maxTimestamp = n.maxTimestamp;
		int threadId = n.getThreadId();

		Set<Node> ret = new HashSet<>();

		for (int t : opMinTimestampMap.keySet()) {
			if (t == threadId)
				continue;

			var entry = opMinTimestampMap.get(t).ceilingEntry(maxTimestamp);
			if (entry != null) {
				Node candidate = entry.getValue().firstEntry().getValue();
				ret.add(candidate);
			}
		}

		return ret;
	}

	// nodes just before n, in real-time, on other threads
	public Set<Node> getNodesJustBefore(Node n) {
		long minTimestamp = n.minTimestamp;
		int threadId = n.getThreadId();

		Set<Node> ret = new HashSet<>();

		for (int t : opMaxTimestampMap.keySet()) {
			if (t == threadId)
				continue;

			var entry = opMaxTimestampMap.get(t).floorEntry(minTimestamp);
			if (entry != null) {
				Node candidate = entry.getValue().lastEntry().getValue();
				ret.add(candidate);
			}
		}

		return ret;
	}

	// min timestamp
	public Iterator<Node> getDescendingUpdates(int threadId, int key, long timestamp, int seq) {
		return new MyIterator(updatesMap, threadId, key, timestamp, seq);
	}

	public static class MyIterator implements Iterator<Node> {
		NavigableMap<Long, NavigableMap<Integer, Node>> threadKeyTimestampMap;
		Iterator<Long> timestampDescIter = null;
		long currentTimestamp;
		Iterator<Integer> seqDescIter = null;

		public MyIterator(
				Map<Integer, Map<Integer, NavigableMap<Long, NavigableMap<Integer, Node>>>> threadKeyTimestampSeqNode,
				int threadId, int key, long timestamp, int seq) {
//			threadKeyTimestampMap = threadKeyTimestampSeqNode.get(threadId).get(key).headMap(timestamp, true);
			if (threadKeyTimestampSeqNode == null || threadKeyTimestampSeqNode.get(threadId) == null)
				return;
			threadKeyTimestampMap = threadKeyTimestampSeqNode.get(threadId).get(key);
			if (threadKeyTimestampMap == null)	
				return;
			threadKeyTimestampMap = threadKeyTimestampMap.headMap(timestamp, true);
			timestampDescIter = threadKeyTimestampMap.descendingKeySet().iterator();
			if (timestampDescIter.hasNext()) {
				currentTimestamp = timestampDescIter.next();
				seqDescIter = threadKeyTimestampMap.get(currentTimestamp).headMap(seq, false).descendingKeySet()
						.iterator();
				hasNext();
			}
		}

		@Override
		public boolean hasNext() {
			if (timestampDescIter == null || seqDescIter == null)
				return false;
			if (seqDescIter.hasNext())
				return true;
			if (timestampDescIter.hasNext()) {
				currentTimestamp = timestampDescIter.next();
				seqDescIter = threadKeyTimestampMap.get(currentTimestamp).descendingKeySet().iterator();
				return true;
			}
			return false;
		}

		@Override
		public Node next() {
			int seq = seqDescIter.next();
			return threadKeyTimestampMap.get(currentTimestamp).get(seq);
		}
	}
}
