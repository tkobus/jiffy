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
import java.util.Set;

import pl.edu.put.concurrent.conctest.LabeledEdge.LabelType;
import pl.edu.put.utils.Pair;

/* istotne sa wszystkie writey wspolbiezne lub cofniete w czasie wzgledem r, \
 * ale do pierwszego takiego (wlacznie), ktory ma polaczenie do r;
 * bierzemy w, ktore ma polaczneie z r; jesli jest jeszcze jakis w' w zbiorze 
 * kandydatow, z ktorym ma polaczenie w, to laczymy r z w (w')?; 
 * ale uwaga, nie ma total orderu na wszystkich zapisach (w/d) na zmiennej, bo 
 * wlasnie sa d
 * 
 * jesli jeden del w 
 * zbiorze a r zwraca null, to d -> r
 */

public class EdgeRefiner {

	public static int evaluateWitnessValue(TraceChecker traceChecker, Node node)
			throws PathDetector.CycleFoundException, CheckerException {
		Trace.Event e = node.event;
		if (e == null || e.type == null)
			return 0;

		long timestamp = node.maxTimestamp;

		Pair<Integer, String>[] witnessValues = node.getWitnessValue();

		int newEdges = 0;

		if (witnessValues != null) {
			for (var p : witnessValues)
				newEdges += helper(traceChecker, node, p.first, p.second, timestamp);
		}

		return newEdges;
	}

	private static int helper(TraceChecker traceChecker, Node node, int key, String value, long timestamp)
			throws PathDetector.CycleFoundException, CheckerException {
		boolean debug = false;

		Set<Node> candidates = getCandidates(traceChecker, node, key, timestamp);
		if (candidates.isEmpty()) {
			String msg = String.format("No candidates!\n- key: %d\n- value: %s\n- timestamp: %d\n- node: %s\n", key,
					value, timestamp, node);
			System.out.println(msg);
			throw new CheckerException(msg);
		}

		int newEdges = 0;
		if (value != null) {
			Node trueWrite = traceChecker.writtenValueMap.get(value);
			if (!candidates.contains(trueWrite)) {
				String msg = "True write is not a candidate!";
				System.out.println(msg);
				throw new CheckerException(msg);
			}

			PathDetector fromTrueWrite = new PathDetector(traceChecker.graph, trueWrite);

			for (Node c : candidates) {
				if (c.equals(trueWrite) || c.equals(node))
					continue;
				try {
					if (fromTrueWrite.pathExistsTo(c)) {// && !d2.pathExists(node, c)) {
						if (traceChecker.addEdge(node, c, debug, false, LabelType.FollowRefinement))
							newEdges++;
					}
				} catch (PathDetector.CycleFoundException e) {
					e.printStackTrace();
					traceChecker.cycleExists();
//					System.err.println("### " + node + "\t" + c);
//					System.err.println(e.getCycle());
					throw e;
				}
			}
		} else {
			Node delete = null;
			for (Node c : candidates) {
				if (c.event == null) {
					delete = null;
					break;
				}
				if (c.event.type.equals(Trace.EventType.Delete)) {
					if (delete != null) {
						delete = null;
						break;
					}
					delete = c;
				} else if (c.event.type.equals(Trace.EventType.Batch)) {
					Trace.BatchEvent be = (Trace.BatchEvent) c.event;
					if (!be.deleteEvents.containsKey(key))
						continue;
					if (delete != null) {
						delete = null;
						break;
					}
					delete = c;
				}
			}

			if (delete != null && !delete.equals(node)) {
				if (traceChecker.addEdge(delete, node, debug, false, LabelType.DeleteRefinement))
					newEdges++;

				PathDetector fromTrueDelete = new PathDetector(traceChecker.graph, delete);

				for (Node c : candidates) {
					if (c.equals(delete) || c.equals(node))
						continue;
					try {
						if (fromTrueDelete.pathExistsTo(c)) {// && !d2.pathExists(node, c)) {
							if (traceChecker.addEdge(node, c, debug, false, LabelType.FollowDeleteRefinement))
								newEdges++;
						}
					} catch (PathDetector.CycleFoundException e) {
						e.printStackTrace();
						traceChecker.cycleExists();
//						System.err.println("### " + node + "\t" + c);
//						System.err.println(e.getCycle());
						throw e;
					}
				}
			}
		}

		return newEdges;
	}

	/*
	 * Candidates is a set of operations that potentially can directly influence the
	 * value of the operation given o. More precisely, the set includes operations
	 * u, such that: - u is on the same key as o - u happened-before o or u is
	 * concurrent with o - there is no path o -> u - there may or may not be path u
	 * -> o - there is no other operation u' such that there exists a path u -> u'
	 * -> o
	 */
	private static Set<Node> getCandidates(TraceChecker traceChecker, Node node, int key, long timestamp)
			throws PathDetector.CycleFoundException {
		var storeKey = Pair.makePair(node, key);
		Map<Node, Boolean> candidates = traceChecker.candidatesStore.get(storeKey);
		Set<Node> toRemove = new HashSet<>();

		if (candidates == null) {
			candidates = new HashMap<>();
			traceChecker.candidatesStore.put(storeKey, candidates);

			PathDetector fromNode = new PathDetector(traceChecker.graph, node);
			for (int t : traceChecker.programOrderOperations.getThreadIds()) {
				Iterator<Node> threadIterator = traceChecker.programOrderOperations.getDescendingUpdates(t, key,
						timestamp, Integer.MAX_VALUE);

				while (threadIterator.hasNext()) {
					Node currentNode = threadIterator.next();
					if (fromNode.pathExistsTo(currentNode))
						continue;

					PathDetector toNode = new PathDetector(traceChecker.graph, currentNode);
					boolean pathExists = toNode.pathExistsTo(node);

					candidates.put(currentNode, pathExists);
					if (pathExists)
						break;
				}
			}
		} else {
//			if (myGraph.FAST_ITERATIONS && myGraph.touchedNodesOld != null && !myGraph.touchedNodesOld.contains(node)) {
//				boolean touched = false;
//				for (Node n : candidates.keySet()) {
//					touched |= myGraph.touchedNodesOld.contains(n);
//				}
//				if (!touched)
//					return candidates.keySet();
//			}

			PathDetector fromNode = new PathDetector(traceChecker.graph, node);
			for (var entry : candidates.entrySet()) {
				Node n = entry.getKey();
				boolean pathExists = entry.getValue();

				if (n.minTimestamp >= timestamp) {
					toRemove.add(n);
					continue;
				}

				if (fromNode.pathExistsTo(n)) {
					toRemove.add(n);
					continue;
				}

				if (!pathExists) {
					PathDetector toNode = new PathDetector(traceChecker.graph, n);
					pathExists = toNode.pathExistsTo(node);
					candidates.put(n, pathExists);
				}
			}
		}

		// there should be no paths between candidates
		for (var e1 : candidates.entrySet()) {
			if (!e1.getValue())
				continue;
			Node c1 = e1.getKey();
			PathDetector overwrittenNode = new PathDetector(traceChecker.graph, c1);
			for (var e2 : candidates.entrySet()) {
				if (!e2.getValue())
					continue;
				Node c2 = e2.getKey();
				if (c1 == c2)
					continue;
				if (overwrittenNode.pathExistsTo(c2)) {
					toRemove.add(c1);
					break;
				}
			}
		}

		candidates.keySet().removeAll(toRemove);

		return candidates.keySet();
	}

	public static Map<Integer, Set<Node>> getCandidates(TraceChecker traceChecker, Node node)
			throws PathDetector.CycleFoundException {
		Pair<Integer, String>[] witnessedValues = node.getWitnessValue();

		if (witnessedValues == null)
			return null;

		Map<Integer, Set<Node>> candidates = new HashMap<>();

		for (var p : witnessedValues) {
			HashSet<Node> nodes = new HashSet<>();
			nodes.addAll(getCandidates(traceChecker, node, p.first, node.maxTimestamp));
			nodes.remove(node);
			var oldNodes = candidates.put(p.first, nodes);
			if (oldNodes != null)
				nodes.addAll(oldNodes);
		}

		return candidates;
	}
}
