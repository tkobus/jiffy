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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.edu.put.concurrent.MultiversionNavigableMap;
import pl.edu.put.concurrent.conctest.ConcurrentMapTest.ScenarioConfiguration;
import pl.edu.put.concurrent.conctest.Trace.BatchEvent;
import pl.edu.put.concurrent.conctest.Trace.DeleteEvent;
import pl.edu.put.concurrent.conctest.Trace.Event;
import pl.edu.put.concurrent.conctest.Trace.GetEvent;
import pl.edu.put.concurrent.conctest.Trace.PutEvent;
import pl.edu.put.concurrent.conctest.Trace.SnapshotEvent;
import pl.edu.put.concurrent.conctest.Trace.TimestampEvent;
import pl.edu.put.concurrent.jiffy.Batch;
import pl.edu.put.concurrent.jiffy.Jiffy;
import pl.edu.put.concurrent.jiffy.RuntimeStatistics;

public class WorkerThread extends Thread {

	private static final Logger logger = LoggerFactory.getLogger(WorkerThread.class);

	int threadId;
	int threads;

	boolean dedicatedSnapshotsRun = false;
	boolean runOnlySnapshots = false;

	// MultiversionNavigableMap<Integer, String> map;
	MultiversionNavigableMap<Integer, String> map;
	ScenarioConfiguration conf;
	Random random;

	Trace trace;
	int seqNum;

	boolean performanceRun;
	int performanceRunHash = 0;

	WorkerStatistics statistics;

	public RuntimeStatistics runtimeStatistics;
	public String runtimeStatisticsStr;

	private volatile boolean quit = false;

	public WorkerThread(int threadId, int threads, MultiversionNavigableMap<Integer, String> map,
			ScenarioConfiguration conf, Random random, boolean performanceRun) {

		this.threadId = threadId;
		this.threads = threads;

		this.dedicatedSnapshotsRun = conf.dedicatedSnapshotThreads > 0;
		this.runOnlySnapshots = dedicatedSnapshotsRun && (1f * threadId / threads) < conf.dedicatedSnapshotThreads;

		this.map = map;
		this.conf = conf;
		this.random = random;

		this.trace = performanceRun ? null : new Trace(threadId);
		this.performanceRun = performanceRun;
		this.seqNum = 0;

		this.statistics = new WorkerStatistics(threadId);
	}

	@Override
	public void run() {
		System.out.format("Thread %d starting\n", threadId);
		try {
			int tsCounter = 0;
			statistics.elapsedMilliseconds = System.currentTimeMillis();
			while (!quit) {
				if (tsCounter % conf.operationsPerTimestamp == 0) {
					long timestamp = System.nanoTime();
					if (trace != null)
						trace.append(new TimestampEvent(timestamp));
				}

				Event e = null;
				Event[] eArray = null;
				if (dedicatedSnapshotsRun) {
					if (runOnlySnapshots) {
						e = doSnapshotOps();
					} else {
						while (true) {
							float n = random.nextFloat();

							if (n <= conf.getFrac) {
								eArray = doGet();
								break;
							} else if (n <= conf.getFrac + conf.putDelFrac) {
								e = doPutDel();
								break;
							} else if (n <= conf.getFrac + conf.putDelFrac + conf.batchFrac) {
								e = doBatch();
								break;
							}
						}
					}
				} else {
					float n = random.nextFloat();

					if (n <= conf.getFrac)
						eArray = doGet();
					else if (n <= conf.getFrac + conf.putDelFrac)
						e = doPutDel();
					else if (n <= conf.getFrac + conf.putDelFrac + conf.batchFrac)
						e = doBatch();
					else
						e = doSnapshotOps();
				}

				if (!performanceRun) {
					if (e != null)
						trace.append(e);
					else {
						for (var ee : eArray)
							trace.append(ee);
					}
				}

				tsCounter++;
			}
		} catch (CheckerException e) {
			throw new RuntimeException(e);
		}

		statistics.elapsedMilliseconds = System.currentTimeMillis() - statistics.elapsedMilliseconds
				+ (performanceRunHash % 2);

		if (map instanceof Jiffy<?, ?>) {
			this.runtimeStatistics = ((Jiffy<Integer, String>) map).getRuntimeStatistics();
			this.runtimeStatisticsStr = this.runtimeStatistics.toString();
		}

		System.out.format("Thread %d joining\n", threadId);
	}

	private Event[] doGet() throws CheckerException {
		boolean pureGet = random.nextFloat() > conf.getIterFrac;
		if (pureGet)
			return new Event[] { doPureGet() };
		else
			return doNoSnapshotIterOps();
	}

	private Event doPureGet() {
		int key = random.nextInt(conf.keys);
		String value = map.get(key);
		logger.info(String.format("get %s %s", key, value));

		statistics.gets++;

		GetEvent e = performanceRun ? null : new GetEvent(key, value);
		performanceRunHash ^= value == null ? 42 : value.hashCode();
		return e;
	}

	private Event doPutDel() {
		int key = random.nextInt(conf.keys);
		boolean performPut = random.nextFloat() <= 0.5f;

		Event e;
		if (performPut) {
			String value = String.format("%d_%d", threadId, seqNum);
			seqNum++;
			String oldValue = map.put(key, value);
			logger.info(String.format("get %s %s (%s)", key, value, oldValue));

			statistics.puts++;

			e = performanceRun ? null : new PutEvent(key, value, oldValue);
			performanceRunHash ^= value == null ? 42 : value.hashCode();
		} else {
			String oldValue = map.remove(key);
			logger.info(String.format("del %s (%s)", key, oldValue));

			statistics.deletes++;

			e = performanceRun ? null : new DeleteEvent(key, oldValue);
			performanceRunHash ^= oldValue == null ? 42 : oldValue.hashCode();
		}
		return e;
	}

	private Event doBatch() {
		logger.info("batch");
		BatchEvent e = performanceRun ? null : new BatchEvent();

		Batch<Integer, String> batch = new Batch<>();
		while (batch.mapSize() < conf.batchSize) {
			int key = random.nextInt(conf.keys);
			boolean performPut = random.nextFloat() <= 0.5f;

			if (performPut) {
				String value = String.format("%d_%d", threadId, seqNum);
				logger.info(String.format("put %s %s", key, value));
				seqNum++;

				batch.put(key, value);
			} else {
				logger.info(String.format("del %s", key));
				batch.remove(key);
			}
		}

		statistics.batches++;
		for (var en : batch.getMap().entrySet()) {
			int key = en.getKey();
			String value = en.getValue();
			if (value != null) {
				if (!performanceRun)
					e.put(key, value);
				statistics.batchPuts++;
			} else {
				if (!performanceRun)
					e.delete(key);
				statistics.batchDeletes++;
			}
		}

		map.put(batch);

		for (var kv : batch.getSubstitutedValues().entrySet()) {
			String oldValue = kv.getValue();
			if (oldValue != null) {
				performanceRunHash ^= oldValue.hashCode();
				if (!performanceRun) {
					e.setOldVersion(kv.getKey(), oldValue);
					logger.info(String.format("oldValue %s %s", kv.getKey(), oldValue));
				}
			} else {
				performanceRunHash ^= 42;
			}

		}

		return e;
	}

	private Event[] doNoSnapshotIterOps() throws CheckerException {
		List<Event> eList = performanceRun ? null : new ArrayList<>();

		int key = random.nextInt(conf.keys);
		boolean ascending = random.nextFloat() > conf.descendingGetIterFrac;

		MultiversionNavigableMap<Integer, String> submap;
		if (ascending)
			submap = map.tailMap(key);
		else
			submap = map.descendingMap().tailMap(key);

		Set<Integer> keys = performanceRun ? null : new HashSet<>();
		List<Map.Entry<Integer, String>> entries = performanceRun ? null : new ArrayList<>();
		int lastKey = ascending ? Integer.MIN_VALUE : Integer.MAX_VALUE;

		int counter = 0;
		var iter = submap.entrySet().iterator();

		if (ascending)
			statistics.ascendingIters++;
		else
			statistics.descendingIters++;

		while (iter.hasNext() && counter < conf.snapIterSize) {
			var entry = iter.next();
			key = entry.getKey();
			String value = entry.getValue();
			performanceRunHash ^= value == null ? 42 : value.hashCode();

			if (!performanceRun) {
				entries.add(entry);

				if (keys.contains(key)) {
					String msg = String.format("ERROR - Regular iter: duplicate key: %d (%s)\n", key, keys);
					throw new CheckerException(msg);
				}
				if (ascending) {
					if (key <= lastKey) {
						String msg = String.format("ERROR - Regular iter: no monotonic keys: %d %d\n%s\n", key, lastKey,
								entries);
						throw new CheckerException(msg);
					}
				} else {
					if (key >= lastKey) {
						String msg = String.format("ERROR - Regular iter: no monotonic keys (desc): %d %d\n%s\n", key,
								lastKey, entries);
						throw new CheckerException(msg);
					}
				}
				lastKey = key;
			}

			statistics.iterGets++;

			if (!performanceRun) {
				eList.add(new GetEvent(key, value));
			}
			counter++;
		}

		return eList.toArray(new Event[eList.size()]);
	}

	private Event doSnapshotOps() throws CheckerException {
		SnapshotEvent e = performanceRun ? null : new SnapshotEvent();

		Map<Integer, Set<String>> recentGets = new HashMap<>();
		String NULL = new String("NULL");
		NavigableMap<Integer, String> refMap = new TreeMap<>();

		statistics.snapshots++;
		var snapshot = map.snapshot();

		if (!performanceRun) {
			for (int key = 0; key < conf.keys; key++) {
				String value = snapshot.get(key);
				String newValue = value == null ? NULL : value;

				Set<String> s = new HashSet<>();
				recentGets.put(key, s);
				s.add(newValue);

				if (value != null)
					refMap.put(key, value);

				e.append(key, value);
				statistics.snapshotGets++;
			}
		}

		for (int i = 0; i < conf.opsPerSnap; i++) {
			float randVal = random.nextFloat();
			boolean simpleGet = randVal <= conf.getsPerSnapFrac;
			boolean relOpCheck = randVal <= conf.getsPerSnapFrac + conf.relOpsSnapFrac;

			if (simpleGet) {
				int key = random.nextInt(conf.keys);
				String value = snapshot.get(key);
				performanceRunHash ^= value == null ? 42 : value.hashCode();

				if (!performanceRun) {
					Set<String> s = recentGets.get(key);
					if (s == null) {
						s = new HashSet<>();
						recentGets.put(key, s);
					}
					String newValue = value == null ? NULL : value;
					if (!s.contains(newValue) && s.size() >= 1) {
						String msg = String.format("ERROR - Snapshot get:\n- key: %s\n- old: %s\n- new: %s\n", key, s,
								newValue);
						throw new CheckerException(msg);
					}
					s.add(newValue);

					e.append(key, value);
				}

				statistics.snapshotGets++;
			} else if (relOpCheck) {
				// should not be used in performance runs
//				NavigableMap<Integer, String> refMap = new TreeMap<>();
//
//				for (int key = 0; key < conf.keys; key++) {
//					String value = snapshot.get(key);
//
//					if (value != null)
//						refMap.put(key, value);
//					e.append(key, value);
//				}

				@SuppressWarnings("rawtypes")
				long version = ((Jiffy.SubMap) snapshot).effectiveVersion();

				for (int key = 0; key < conf.keys; key++) {
					var floorEntry = snapshot.floorEntry(key);
					var ceilingEntry = snapshot.ceilingEntry(key);
					var higherEntry = snapshot.higherEntry(key);
					var lowerEntry = snapshot.lowerEntry(key);
					var firstEntry = snapshot.firstEntry();
					var lastEntry = snapshot.lastEntry();

					var refFloorEntry = refMap.floorEntry(key);
					var refCeilingEntry = refMap.ceilingEntry(key);
					var refHigherEntry = refMap.higherEntry(key);
					var refLowerEntry = refMap.lowerEntry(key);
					var refFirstEntry = refMap.firstEntry();
					var refLastEntry = refMap.lastEntry();

					@SuppressWarnings("unchecked")
					Map.Entry<Integer, String>[] entries = new Map.Entry[] { floorEntry, ceilingEntry, higherEntry,
							lowerEntry, firstEntry, lastEntry };
					@SuppressWarnings("unchecked")
					Map.Entry<Integer, String>[] refEntries = new Map.Entry[] { refFloorEntry, refCeilingEntry,
							refHigherEntry, refLowerEntry, refFirstEntry, refLastEntry };

					for (int eIndex = 0; eIndex < entries.length; eIndex++) {
						var entry = entries[eIndex];
						var refEntry = refEntries[eIndex];

						if (entry != null && !performanceRun)
							e.append(entry.getKey(), entry.getValue());

						StringBuilder builder = new StringBuilder();

						if (!(entry == null && refEntry == null) && ((entry == null && refEntry != null)
								|| (entry != null && refEntry == null) || (!entry.getKey().equals(refEntry.getKey()))
								|| (!entry.getValue().equals(refEntry.getValue())))) {

							builder.append(String.format("Map: version: %d, refMapSize: %d\n", version, refMap.size()));
							for (var ee : refMap.entrySet())
								builder.append(String.format("- %d=%s\n", ee.getKey(), ee.getValue()));

							builder.append(
									String.format(
											"ERROR - RelOp refEntry (eIndex: %d): (iteration: %d) key: %d version: %d\n"
													+ "- ref: %s\n" + "- new: %s\n",
											eIndex, i, key, version, refEntry, entry));

							String msg = builder.toString();
							throw new CheckerException(msg);
						}
						statistics.snapshotRelOps++;
					}
				}

				int key = random.nextInt(conf.keys);
				var tailMap = snapshot.tailMap(key);

				var iter = tailMap.entrySet().iterator();
				var refIter = refMap.tailMap(key).entrySet().iterator();

				while (refIter.hasNext()) {
					statistics.snapshotRelOps++;
					if (!iter.hasNext()) {
						String msg = String.format(
								"ERROR - RelOp iter (refIter.hasNext() && !iter.hasNext()) key: %s, refMap: %s\nrefEntry: %s\n",
								key, refMap.tailMap(key).toString(), refIter.next());
						throw new CheckerException(msg);
					}
					var entry = iter.next();
					var refEntry = refIter.next();

					if (!entry.equals(refEntry)) {
						String msg = String.format(
								"ERROR - RelOp iter key: %s, refMap: %s\nceilingEntry(key)%s\nentry/refEntry: %s %s\nnull: %s\n",
								key, refMap.tailMap(key).toString(), snapshot.ceilingEntry(key), entry, refEntry, null);// history);
						throw new CheckerException(msg);
					}
				}

				key = random.nextInt(conf.keys);
				var descendingMap = snapshot.descendingMap();
				tailMap = descendingMap.tailMap(key);

				iter = tailMap.entrySet().iterator();
				refIter = refMap.descendingMap().tailMap(key).entrySet().iterator();

				while (refIter.hasNext()) {
					statistics.snapshotRelOps++;
					if (!iter.hasNext()) {
						String msg = String.format(
								"ERROR - RelOp desc iter (refIter.hasNext() && !iter.hasNext()) key: %s, refMap: %s\nrefEntry: %s\n",
								key, refMap.descendingMap().tailMap(key).toString(), refIter.next());
						throw new CheckerException(msg);
					}
					var entry = iter.next();
					var refEntry = refIter.next();

					if (!entry.equals(refEntry)) {
						String msg = String.format(
								"ERROR - RelOp desc iter key: %s, refMap: %s\nceilingEntry(key): %s\nentry/refEntry: %s %s\nnull: %s\n",
								key, refMap.descendingMap().tailMap(key).toString(), descendingMap.ceilingEntry(key),
								entry, refEntry, null);// history);
						throw new CheckerException(msg);
					}
				}
			} else {
				int chosenKey = random.nextInt(conf.keys);
				boolean ascending = random.nextFloat() > conf.descendingIterSnapFrac;

				MultiversionNavigableMap<Integer, String> submap;
				SortedMap<Integer, String> localRefMap = null;
				if (ascending) {
					submap = snapshot.tailMap(chosenKey);
					localRefMap = refMap.tailMap(chosenKey);
				} else {
					submap = snapshot.descendingMap().tailMap(chosenKey);
					localRefMap = refMap.descendingMap().tailMap(chosenKey);
				}

				Set<Integer> keys = performanceRun ? null : new HashSet<>();
				List<Map.Entry<Integer, String>> entries = performanceRun ? null : new ArrayList<>();
				int lastKey = ascending ? Integer.MIN_VALUE : Integer.MAX_VALUE;

				int counter = 0;
				var iter = submap.entrySet().iterator();
				var refIter = !performanceRun ? localRefMap.entrySet().iterator() : null;
				if (ascending)
					statistics.snapshotIters++;
				else
					statistics.snapshotDescendingIters++;

				while (iter.hasNext() && counter < conf.snapIterSize) {
					var entry = iter.next();
					Integer key = entry.getKey();
					String value = entry.getValue();
					performanceRunHash ^= value == null ? 42 : value.hashCode();

					if (!performanceRun) {
						entries.add(entry);

						var refEntry = refIter.hasNext() ? refIter.next() : null;
						if (true && refEntry == null) {
							@SuppressWarnings("rawtypes")
							var history = ((Jiffy.SubMap.SubMapIter) iter).debugGetHistory();
							String msg = String.format(
									"ERROR - Iterator: refIter is null:\n- chosenKey: %d\n- refMap: %s\n- entry: %d=%s\n- history:\n%s\n",
									chosenKey, refMap, key, value, history);
							throw new CheckerException(msg);
						}

						var refKey = refEntry.getKey();
						var refValue = refEntry.getValue();

						if (true && (!key.equals(refKey) || !value.equals(refValue))) {
							@SuppressWarnings("rawtypes")
							var history = ((Jiffy.SubMap.SubMapIter) iter).debugGetHistory();
							String msg = String.format(
									"ERROR - Iterator: refIter doesn't match:\n- chosenKey: %d\n- refMap: %s\n- refEntry: %d=%s\n- entry: %d=%s\n- oneMoreTime: %d=%s, %d=%s\n- history:\n%s\n",
									chosenKey, refMap, refKey, refValue, key, value, refKey, snapshot.get(refKey), key,
									snapshot.get(key), history);
							try {
								sleep(1);
							} catch (Exception exc) {
							}
							;
							var iter2 = submap.entrySet().iterator();
							var entry2 = iter2.next();
							Integer key2 = entry2.getKey();
							String value2 = entry2.getValue();
							while (key2 < refKey) {
								msg += String.format("%d=%s, ", key2, value2);
								entry2 = iter2.next();
								key2 = entry2.getKey();
								value2 = entry2.getValue();
							}
							msg += String.format("%d=%s\n", key2, value2);
							throw new CheckerException(msg);
						}

						if (keys.contains(key)) {
							String msg = String.format("ERROR - Iterator: Duplicate key: %d (%s)\n", key, keys);
							throw new CheckerException(msg);
						}
						if (ascending) {
							if (key <= lastKey) {
								String msg = String.format("ERROR - Iterator: No monotonic keys: %d %d\n%s\n", key,
										lastKey, entries);
								throw new CheckerException(msg);
							}
						} else {
							if (key >= lastKey) {
								String msg = String.format("ERROR - Iterator: No monotonic keys (desc): %d %d\n%s\n",
										key, lastKey, entries);
								throw new CheckerException(msg);
							}
						}
						lastKey = key;

						Set<String> s = recentGets.get(key);
						if (s == null) {
							s = new HashSet<>();
							recentGets.put(key, s);
						}
						String newValue = value == null ? NULL : value;
						if (!s.contains(newValue) && s.size() >= 1) {
							String msg = String.format("ERROR - Iterator sets - key: %s\n- old: %s\n- new: %s\n", key,
									s, newValue);
							throw new CheckerException(msg);
						}
						s.add(newValue);
					}

					statistics.snapshotItersGets++;
					if (!performanceRun)
						e.append(key, value);
					counter++;
				}
			}
		}

		if (!performanceRun) {
			for (var entry : recentGets.entrySet()) {
				int key = entry.getKey();
				String value = (String) entry.getValue().toArray()[0];
				String value2 = snapshot.get(key);
				String newValue = value2 == null ? NULL : value2;

				if (!newValue.equals(value)) {
					String msg = String.format("ERROR - Final snapshot get - key: %s\n- old: %s\n- new: %s\n", key,
							value, newValue);
					throw new CheckerException(msg);
				}
				statistics.snapshotGets++;
			}
		}

		snapshot.close();

		return e;
	}

	public void quit() {
		this.quit = true;
	}
}
