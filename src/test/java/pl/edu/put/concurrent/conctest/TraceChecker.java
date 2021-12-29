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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.jgrapht.Graph;
import org.jgrapht.alg.cycle.CycleDetector;
import org.jgrapht.graph.DirectedMultigraph;
import org.jgrapht.traverse.DepthFirstIterator;
import pl.edu.put.concurrent.conctest.LabeledEdge.LabelType;
import pl.edu.put.concurrent.conctest.PathDetector.CycleFoundException;
import pl.edu.put.concurrent.conctest.Trace.BatchEvent;
import pl.edu.put.concurrent.conctest.Trace.DeleteEvent;
import pl.edu.put.concurrent.conctest.Trace.Event;
import pl.edu.put.concurrent.conctest.Trace.EventType;
import pl.edu.put.concurrent.conctest.Trace.GetEvent;
import pl.edu.put.concurrent.conctest.Trace.PutEvent;
import pl.edu.put.concurrent.conctest.Trace.SnapshotEvent;
import pl.edu.put.concurrent.conctest.Trace.TimestampEvent;
import pl.edu.put.utils.Pair;

public class TraceChecker {

	final static boolean FAST_ITERATIONS = false;

	public enum Mode {
		NO_CYCLE_DETECTION(0), SHORT(1), FULL(2);

		private int ord;

		private Mode(int ord) {
			this.ord = ord;
		}

		public static Mode getMode(int ord) {
			switch (ord) {
			case 0:
				return NO_CYCLE_DETECTION;
			case 1:
				return SHORT;
			case 2:
				return FULL;
			default:
				throw new IllegalArgumentException();
			}
		}
	}

	List<Trace> traces;
	Map<String, Node> writtenValueMap;
	Map<String, Node> overwrittenValueMap;
	Map<Integer, NavigableMap<Long, Node>> threadTimestampNodes;
	Graph<Node, LabeledEdge> graph;
	Node root;
	ProgramOrderOperations programOrderOperations;

	public TraceChecker(List<Trace> traces) {
		this.traces = traces;
		writtenValueMap = new HashMap<>();
		overwrittenValueMap = new HashMap<>();
		graph = new DirectedMultigraph<>(LabeledEdge.class);
	}

	protected boolean addEdge(Node sourceVertex, Node targetVertex, LabelType label) {
		return addEdge(sourceVertex, targetVertex, false, false, label);
	}

	protected boolean addEdge(Node sourceVertex, Node targetVertex, boolean debug, boolean force, LabelType label) {
		if (force) {
			graph.addEdge(sourceVertex, targetVertex);
			return true;
		} else {
			Set<LabeledEdge> edges = graph.getAllEdges(sourceVertex, targetVertex);
			if (edges.size() == 0) {
				graph.addEdge(sourceVertex, targetVertex, new LabeledEdge(label));
				if (FAST_ITERATIONS) {
					touchedNodesNew.add(sourceVertex);
					touchedNodesNew.add(targetVertex);
				}
				if (debug)
					System.out.println("EDGE: " + sourceVertex + "\t" + targetVertex);
				return true;

			}
			return false;
		}
	}

	public void run(Mode mode) {
		try {
			init(mode);
			if (mode != Mode.FULL)
				return;
//		candidatesHistogram();
			check(mode);
			candidatesHistogram();
		} catch (CheckerException e) {
			System.err.format("Checker exception: %s. Exiting.\n", e.getMessage());
			System.exit(1);
		}
	}

	private void init(Mode mode) throws CheckerException {
		System.out.println("Starting the checker.\n");

		touchedNodesOld = null;
		touchedNodesNew = new HashSet<Node>();
		candidatesStore = new HashMap<>();

		boolean atomicSnapshots = checkSnapshotSameReadValues(traces);
		System.out.format("Atomic snapshots: %s\n", atomicSnapshots);

		if (!atomicSnapshots)
			throw new CheckerException("no atomic snapshots");

		boolean monotonicWrites = checkMonotonicWrites(traces);
		System.out.format("Monotonic writes: %s\n", monotonicWrites);

		if (!monotonicWrites)
			throw new CheckerException("no monotonic writes");

		boolean uniqueValues = checkUniqueValues(traces);
		System.out.format("Unique values: %s\n\n", uniqueValues);

		if (!uniqueValues)
			throw new CheckerException("no unique values");

		if (mode == Mode.NO_CYCLE_DETECTION)
			return;

		System.out.println("Initializing a graph.\n");

		root = new Node(null);
		graph.addVertex(root);
		threadTimestampNodes = new HashMap<>();

		for (int i = 0; i < traces.size(); i++) {
			Trace t = traces.get(i);
			if (t.events.size() > 0)
				assert t.events.get(0).threadId == i;

			NavigableMap<Long, Node> timestampNodes = new TreeMap<>();
			threadTimestampNodes.put(i, timestampNodes);

			long timestamp = 0;
			List<Node> nodesSinceLastTimestamp = new ArrayList<>();
			Node currentNode = root;

			// program order + initial min/max timestamps + initializing
			// written/overwrittenValuesMaps
			for (Event e : t.events) {
				Node newNode = null;

				if (e.type.equals(EventType.Timestamp)) {
					TimestampEvent ee = (TimestampEvent) e;
					timestamp = ee.timestamp;
					newNode = timestampNodes.get(timestamp);
					if (newNode == null) {
						newNode = new Node(e);
						newNode.minTimestamp = timestamp;
						newNode.maxTimestamp = timestamp + 1;
						graph.addVertex(newNode);
						addEdge(currentNode, newNode, LabelType.ProgramOrder);
						timestampNodes.put(timestamp, newNode);
					}

					for (Node n : nodesSinceLastTimestamp)
						n.maxTimestamp = timestamp + 1;
					nodesSinceLastTimestamp.clear();
				} else {
					newNode = new Node(e);
					graph.addVertex(newNode);

					newNode.minTimestamp = timestamp;
					nodesSinceLastTimestamp.add(newNode);

					Pair<Integer, String>[] writtenValues = newNode.getWrittenValues();
					if (writtenValues != null) {
						for (Pair<Integer, String> p : writtenValues) {
							Node originalNode = writtenValueMap.put(p.second, newNode);
							if (originalNode != null) {
								System.out.format("Value %s of key %d, written by %s and %s\n", p.second, p.first,
										originalNode, newNode);
							}
						}
					}

					if (newNode.isUpdating()) {
						Pair<Integer, String>[] witnessValues = newNode.getWitnessValue();
						if (witnessValues != null) {
							for (Pair<Integer, String> p : witnessValues) {
								if (p.second == null)
									continue;

								Node originalNode = overwrittenValueMap.put(p.second, newNode);
								if (originalNode != null) {
									System.out.format("Value %s of key %d, overwritten by %s and %s\n", p.second,
											p.first, originalNode, newNode);
								}
							}
						}
					}

					addEdge(currentNode, newNode, LabelType.ProgramOrder);
				}
				currentNode = newNode;
			}
		}
		System.out.format("Vertices: %d\n", graph.vertexSet().size());
		System.out.format("Program-order edges: %d\n", graph.edgeSet().size());

		// timestamp order
		int count = addTimestampEdges();
		System.out.format("Timestamp-order edges: %d\n", count);

		count = addWitnessedValuesEdges();
		System.out.format("Witnessed values edges: %d\n", count);

		count = addOverwrittenValuesEdges();
		System.out.format("Overwritten values edges: %d\n\n", count);

		System.out.format("Vertexes: %d\n", graph.vertexSet().size());
		System.out.format("Edges: %d\n\n", graph.edgeSet().size());

		if (cycleExists())
			throw new CheckerException("cycle in the graph");

		if (mode == Mode.SHORT)
			return;

		// testProgramOrderUpdates = new ProgramOrderUpdates(graph);

//		traverseGraph(root);
		System.out.println("\nRecalculating timestamps:");
		while (true) {
			int[] ret = recalculateMinMaxTimestamps(false);
			int timestampChanges = ret[0];
			int newEdges = ret[1];
			System.out.format("* %d (%d)\n", timestampChanges, newEdges);
			if (timestampChanges == 0)
				break;
		}

		System.out.println("\nBuilding ProgramOrderOperations structure");
		programOrderOperations = new ProgramOrderOperations(graph);

		count = addInternodeTimestampEdges();
		System.out.format("\nNew timestamp edges: %d\n", count);

		System.out.println();
		if (cycleExists())
			throw new CheckerException("cycle in the graph");

		System.out.println("\nGraph initialization done.\n");
	}

	private int addTimestampEdges() {
		int count = 0;
		// for each thread
		for (var map : threadTimestampNodes.values()) {
			// any other thread
			for (var innerMap : threadTimestampNodes.values()) {
				if (map.equals(innerMap))
					continue;
				for (var e : map.entrySet()) {
					var ie = innerMap.higherEntry(e.getKey());
					if (ie != null) {
						addEdge(e.getValue(), ie.getValue(), LabelType.Timestamp);
						// System.out.println(e.getValue() + " -- " + ie.getValue());
						count++;
					}
				}
			}
		}
		return count;
	}

	// written values -> witness values
	// * written values -> read values
	// * written values -> oldValues of new writes
	// * written values -> oldValues of new deletes
	private int addWitnessedValuesEdges() {
		int count = 0;
		for (Node n : graph.vertexSet()) {
			Pair<Integer, String>[] witnessValues = n.getWitnessValue();
			if (witnessValues == null)
				continue;

			for (var p : witnessValues) {
				Node writeNode = writtenValueMap.get(p.second);
				if (writeNode != null && addEdge(writeNode, n, LabelType.Witness))
					count++;
			}
		}

		return count;
	}

	private int addOverwrittenValuesEdges() {
		int count;
		count = 0;
		// read values -> overwritten values by some nodes
		// * read/v -> write/v'/v
		// * read/v -> delete/v
		for (Node n : graph.vertexSet()) {
			if (n.isUpdating())
				continue;

			Pair<Integer, String>[] witnessValues = n.getWitnessValue();
			if (witnessValues == null)
				continue;

			for (var p : witnessValues) {
				Node overwritingNode = overwrittenValueMap.get(p.second);
				if (overwritingNode != null && addEdge(n, overwritingNode, LabelType.Overwritten))
					count++;
			}
		}

		return count;
	}

	private int addInternodeTimestampEdges() {
		int count = 0;

		List<Pair<Node, Node>> edgesToAdd = new ArrayList<>();

		for (Node n : graph.vertexSet()) {
//			Set<Node> candidates;
			Set<Node> candidates = programOrderOperations.getNodesJustBefore(n);
			if (candidates != null) {
				for (Node c : candidates)
					edgesToAdd.add(Pair.makePair(c, n));
			}

			candidates = programOrderOperations.getNodesJustAfter(n);
			if (candidates != null) {
				for (Node c : candidates)
					edgesToAdd.add(Pair.makePair(n, c));
			}
		}

		for (var p : edgesToAdd) {
			if (addEdge(p.first, p.second, LabelType.InitTimestampRefinment)) {
				if (!(p.first.maxTimestamp <= p.second.minTimestamp)) {
					System.out.println(p.first + " -- " + p.second);
					throw new CheckerInternalException("timestamps malformed");
				}
				count++;
			}
		}

		return count;
	}

	boolean cycleExists() {
		return cycleExists("Cycle exists:");
	}

	private boolean cycleExists(String text) {
		CycleDetector<Node, LabeledEdge> detector = new CycleDetector<>(graph);
		boolean cycle = detector.detectCycles();
		System.out.format("%s %s\n", text, cycle);
		Set<Node> cycleSet = detector.findCycles();
		for (Node n : cycleSet) {
			System.out.println("- " + n);
			Set<LabeledEdge> edges = graph.outgoingEdgesOf(n);
			for (LabeledEdge e : edges) {
				Node target = graph.getEdgeTarget(e);
				if (!cycleSet.contains(target))
					continue;
				System.out.format("  o: %s (%s)\n", (target.event != null ? target.event.getSignature() : "null"),
						e.toStringShort());
			}
			edges = graph.incomingEdgesOf(n);
			for (LabeledEdge e : edges) {
				Node source = graph.getEdgeSource(e);
				if (!cycleSet.contains(source))
					continue;
				System.out.format("  i: %s (%s)\n", (source.event != null ? source.event.getSignature() : "null"),
						e.toStringShort());
			}
		}

//		if (cycle) {
////			System.out.println("\nDUMP:");
////			for (Node n : graph.vertexSet()) {
////				System.out.println("- " + n);
////			}
////			for (LabeledEdge e : graph.edgeSet()) {
////				Node source = graph.getEdgeSource(e);
////				Node target = graph.getEdgeTarget(e);
////				System.out.format("* %s -- %s : %s\n", (source.event != null ? source.event.getSignature() : "null"),
////						(target.event != null ? target.event.getSignature() : "null"), e.toStringShort());
////			}
//		} else {
//			if (ConcurrentMapTest.TESTS_MODE >= 1) {
//				System.out.println("No cycle, ending for now.");
//				System.exit(0);
//			}
//		}
		return cycle;
	}

	Set<Node> touchedNodesOld;
	Set<Node> touchedNodesNew;

	// combination of Node and Key -> set of candidates together with a flag whether
	// a node is connected to the read
	Map<Pair<Node, Integer>, Map<Node, Boolean>> candidatesStore;

	private void check(Mode mode) throws CheckerException {
		// Tmp.pathTests(graph);
		// Tmp.descUpdatesTest(this);
		// Tmp.comparePOU(this);

		System.out.println("\nRefining:");

		int i = 0;

		while (true) {
			System.out.format("\niter: %3d\n", i);
			int timestampChanges = 0;
			int newWitnessEdges = 0;
			int newTimestampEdges = 0;
			try {
				for (Node n : graph.vertexSet()) {
					newWitnessEdges += EdgeRefiner.evaluateWitnessValue(this, n);
				}
			} catch (CycleFoundException e) {
				e.printStackTrace();
//				System.out.println(e.getCycle());
			}
			System.out.format("- new follow/delete edges: %5d\n", newWitnessEdges);
			System.out.format("- recalculating timestamps:\n");
			while (true) {
				int[] ret = recalculateMinMaxTimestamps(true);
				timestampChanges += ret[0];
				newTimestampEdges += ret[1];
				System.out.format("  * %d (%d)\n", ret[0], ret[1]);
				if (ret[0] == 0)
					break;
			}

//			CycleDetector<Node, DefaultEdge> detector = new CycleDetector<>(graph);
//			boolean cycle = detector.detectCycles();
//			System.out.format("- cycle exists: %s\n", cycle);
//			if (cycle) {
//				Set<Node> cycleSet = detector.findCycles();
//				for (Node n : cycleSet) {
//					System.out.println("- " + n);
//				}
//				System.exit(1);
//			}

			boolean cycle = cycleExists("- cycle exists:");
			if (cycle)
				throw new CheckerException("cycle exists in the graph");

//			System.out.format("- vertexes: %d\n", graph.vertexSet().size());
			System.out.format("- edges: %d\n\n", graph.edgeSet().size());

			if (cycle || (timestampChanges == 0 && newWitnessEdges == 0 && newTimestampEdges == 0))
				break;

			i++;
			if (FAST_ITERATIONS) {
				touchedNodesOld = touchedNodesNew;
				touchedNodesNew = new HashSet<Node>();
			}
		}
	}

	private void traverseGraph(Node start) {
		Iterator<Node> iterator = new DepthFirstIterator<>(graph, start);
		while (iterator.hasNext()) {
			Node uri = iterator.next();
			System.out.println(uri);
		}
	}

	private int[] recalculateMinMaxTimestamps(boolean updateThreadObjectMinTimestampNodeMap) {
		int timestampChanges = 0;
		int newEdges = 0;

		List<Pair<Node, Node>> edgesToAdd = new ArrayList<>();
		Set<LabeledEdge> edges = graph.edgeSet();
		for (LabeledEdge e : edges) {
			Node source = graph.getEdgeSource(e);
			Node target = graph.getEdgeTarget(e);

			if (FAST_ITERATIONS && touchedNodesOld != null && !touchedNodesOld.contains(source)
					&& !touchedNodesOld.contains(target) && !touchedNodesNew.contains(source)
					&& !touchedNodesNew.contains(target))
				continue;

			if (source.minTimestamp > target.minTimestamp) {
				if (updateThreadObjectMinTimestampNodeMap && target.event != null) {
					programOrderOperations.updateMinTimestamp(target, source.minTimestamp);
					if (FAST_ITERATIONS)
						touchedNodesNew.add(target);
					timestampChanges++;

					Set<Node> candidates = programOrderOperations.getNodesJustBefore(target);
					for (Node c : candidates)
						edgesToAdd.add(Pair.makePair(c, target));
				} else {
					// normally it happens in poo.updateMinTimestamp;
					target.minTimestamp = source.minTimestamp;
					if (FAST_ITERATIONS)
						touchedNodesNew.add(target);
					timestampChanges++;
				}
			}

			if (source.maxTimestamp > target.maxTimestamp) {
				if (updateThreadObjectMinTimestampNodeMap && target.event != null) {
					programOrderOperations.updateMaxTimestamp(source, target.maxTimestamp);
					if (FAST_ITERATIONS)
						touchedNodesNew.add(source);
					timestampChanges++;

					Set<Node> candidates = programOrderOperations.getNodesJustAfter(source);
					for (Node c : candidates)
						edgesToAdd.add(Pair.makePair(source, c));
				} else {
					// normally it happens in poo.updateMaxTimestamp;
					source.maxTimestamp = target.maxTimestamp;
					if (FAST_ITERATIONS)
						touchedNodesNew.add(source);
					timestampChanges++;
				}
			}
		}

		for (var p : edgesToAdd) {
			addEdge(p.first, p.second, LabelType.TimestampRefinement);
			newEdges++;
		}

		return new int[] { timestampChanges, newEdges };
	}

	private boolean checkSnapshotSameReadValues(List<Trace> traces) {
		boolean success = true;
		String NULL = "NULL";

		outer: for (Trace trace : traces) {
			for (Event event : trace.events) {
				if (!event.type.equals(EventType.Snapshot))
					continue;

				SnapshotEvent e = (SnapshotEvent) event;
				Map<Integer, Set<String>> readValues = new HashMap<>();

				for (GetEvent ge : e.getEvents) {
					Set<String> s = readValues.get(ge.key);
					if (s == null) {
						s = new HashSet<>();
						readValues.put(ge.key, s);
					}
					String newValue = ge.value == null ? NULL : ge.value;
					if (!s.contains(newValue) && s.size() >= 1) {
						success = false;
						System.out.println(e);
						System.out.format("problematic key: %d - %s, %s\n", ge.key, s, ge.value);
						break outer;
					}
					s.add(newValue);
				}
			}
		}
		return success;
	}

	private boolean checkMonotonicWrites(List<Trace> traces) {
		boolean success = true;

		int traceThreadId = 0;
		outer: for (Trace trace : traces) {
			LastSeenValueMapper mapper = new LastSeenValueMapper(traceThreadId);
			for (Event event : trace.events) {
				boolean innerSuccess = true;
				switch (event.type) {
				case Get: {
					GetEvent e = (GetEvent) event;
					int key = e.key;
					String s = e.value;
					innerSuccess = mapper.evaluateSeenValue(key, s);
					break;
				}
				case Put: {
					PutEvent e = (PutEvent) event;
					int key = e.key;
					String s = e.oldValue;
					innerSuccess = mapper.evaluateSeenValue(key, s);
					break;
				}
				case Delete: {
					DeleteEvent e = (DeleteEvent) event;
					int key = e.key;
					String s = e.oldValue;
					innerSuccess = mapper.evaluateSeenValue(key, s);
					break;
				}
				case Batch: {
					BatchEvent e = (BatchEvent) event;
					for (var ee : e.putEvents.entrySet()) {
						int key = ee.getKey();
						String s = ee.getValue().oldValue;
						innerSuccess = mapper.evaluateSeenValue(key, s);
						if (!innerSuccess)
							break;
					}
					for (var ee : e.deleteEvents.entrySet()) {
						int key = ee.getKey();
						String s = ee.getValue().oldValue;
						innerSuccess = mapper.evaluateSeenValue(key, s);
						if (!innerSuccess)
							break;
					}
					break;
				}
				case Snapshot: {
					SnapshotEvent e = (SnapshotEvent) event;
					for (var ee : e.getEvents) {
						int key = ee.key;
						String s = ee.value;
						innerSuccess = mapper.evaluateSeenValue(key, s);
						if (!innerSuccess)
							break;
					}
					break;
				}
				default:
					continue;
				}
				if (!innerSuccess) {
					success = false;
					break outer;
				}
			}
			traceThreadId++;
		}
		return success;
	}

	private boolean checkUniqueValues(List<Trace> traces) {
		HashMap<Integer, Integer> deletesPerformed = new HashMap<>();
		HashMap<Integer, Integer> deletesOverwritten = new HashMap<>();
		HashMap<Integer, Set<String>> uniqueValues = new HashMap<>();
		HashMap<Integer, Set<String>> valuesOverwritten = new HashMap<>();
		HashSet<Integer> allKeys = new HashSet<>();

		int traceThreadId = 0;
		for (Trace trace : traces) {
			for (Event event : trace.events) {
				if (!event.isUpdating())
					continue;
				Set<Integer> keys = Arrays.stream(event.getUpdatedKeys()).mapToObj(x -> Integer.valueOf(x))
						.collect(Collectors.toSet());
				allKeys.addAll(keys);
				var writtenValues = event.getWrittenValues();
				if (writtenValues != null) {
					for (var pair : writtenValues) {
						var set = uniqueValues.get(pair.first);
						if (set == null) {
							set = new HashSet<String>();
							uniqueValues.put(pair.first, set);
						}
						if (!set.add(pair.second)) {
							System.out.println(1);
							return false;
						}
						keys.remove(pair.first);
					}
				}
				for (var pair : event.getWitnessValues()) {
					if (pair.second != null) {
						var set = valuesOverwritten.get(pair.first);
						if (set == null) {
							set = new HashSet<String>();
							valuesOverwritten.put(pair.first, set);
						}
						if (!set.add(pair.second)) {
							System.out.println(2);
							System.out.format("%d %d %s %s\n", traceThreadId, pair.first, pair.second,
									set.contains(pair.second));
							System.out.println("- event: " + event);

							for (Trace localTrace : traces) {
								for (Event localEvent : localTrace.events) {
									if (localEvent.getWitnessValues() != null) {
										for (var p : localEvent.getWitnessValues()) {
											if (p != null && p.second != null && p.second.equals(pair.second)) {
												System.out.format("- back event: %s\n", localEvent);
											}
										}
									}
								}
							}

							for (Trace localTrace : traces) {
								for (Event localEvent : localTrace.events) {
									if (localEvent.getWrittenValues() != null) {
										for (var p : localEvent.getWrittenValues()) {
											if (p != null && p.second != null && p.second.equals(pair.second)) {
												System.out.format("- orig event: %s\n", localEvent);
											}
										}
									}
								}
							}

							return false;
						}
					} else {
						var count = deletesOverwritten.getOrDefault(pair.first, 0);
						deletesOverwritten.put(pair.first, count + 1);
					}
				}
				for (var key : keys) {
					var count = deletesPerformed.getOrDefault(key, 0);
					deletesPerformed.put(key, count + 1);
				}
			}
			traceThreadId++;
		}

		for (var key : allKeys) {
			var set1 = uniqueValues.get(key);
			var set2 = valuesOverwritten.get(key);
			if (set1 == null || set2 == null) {
				if ((set1 != null || set2 != null) && set1.size() > 1) {
					System.out.println(
							3 + " " + key + " " + uniqueValues.get(key) + " " + (set1 == null) + " " + (set2 == null));
					return false;
				}
				continue;
			}
			int diff = set1.size() - set2.size();
			if (diff != 0 && diff != 1) {
				System.out.println(4);
				System.out.println("key: " + key);
				System.out.println("set1: " + set1.size());
				System.out.println("set2: " + set2.size());

				set1.removeAll(set2);
				Set<String> diffSet = set1;

				for (String s : diffSet) {

					System.out.println("value: " + s);

					for (Trace localTrace : traces) {
						for (Event localEvent : localTrace.events) {
							if (localEvent.getWitnessValues() != null) {
								for (var p : localEvent.getWitnessValues()) {
									if (p != null && p.second != null && p.second.equals(s)) {
										System.out.format("- witness event: %s\n", localEvent);
									}
								}
							}
						}
					}

					for (Trace localTrace : traces) {
						for (Event localEvent : localTrace.events) {
							if (localEvent.getWrittenValues() != null) {
								for (var p : localEvent.getWrittenValues()) {
									if (p != null && p.second != null && p.second.equals(s)) {
										System.out.format("- orig event: %s\n", localEvent);
									}
								}
							}
						}
					}
				}

				return false;
			}

			int diff2 = deletesPerformed.getOrDefault(key, 0) - deletesOverwritten.getOrDefault(key, 0);
			if (diff2 != -1 && diff2 != 0) {
				System.out.println(5);
				return false;
			}

			if (diff == 1 && diff2 == 0) {
				System.out.println(6);
				return false;
			}
		}

		return true;
	}

	private void candidatesHistogram() throws CheckerException {
		NavigableMap<Integer, Integer> candidatesCountHistogram = new TreeMap<>();
		int totalReads = 0;
		try {
			for (Node n : graph.vertexSet()) {
				Map<Integer, Set<Node>> candidates = EdgeRefiner.getCandidates(this, n);
				if (candidates == null)
					continue;
				totalReads += candidates.size();
				for (var entry : candidates.entrySet()) {
					// int key = entry.getKey();
					var nodes = entry.getValue();
					int count = nodes.size();
					Integer oldCount = candidatesCountHistogram.get(count);
					if (oldCount == null)
						candidatesCountHistogram.put(count, 1);
					else
						candidatesCountHistogram.put(count, oldCount + 1);
				}
			}
		} catch (CycleFoundException e) {
			boolean cycle = cycleExists();
			if (cycle)
				throw new CheckerException("cycle exists in the graph");
		}

		System.out.println("\nCandidates histogram:");
		for (var e : candidatesCountHistogram.entrySet()) {
			var value = e.getValue();
			System.out.format("- %d: %10d %5.2f\n", e.getKey(), value, 100.0 * value / totalReads);
		}
		System.out.println();
	}
}
