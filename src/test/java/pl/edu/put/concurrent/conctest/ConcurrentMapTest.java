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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import pl.edu.put.concurrent.MultiversionNavigableMap;
import pl.edu.put.concurrent.jiffy.Jiffy;
import pl.edu.put.concurrent.jiffy.RuntimeStatistics;

import static java.util.Collections.emptyList;

/**
 * This is the main class of a tool, referred to as checker, which we developed
 * to find bugs in Jiffy, our concurrent map. With a few changes it can be used
 * to aid development of different, linearizable concurrent data structures.
 * Below we describe the main idea behind the checker.
 * 
 * Since we focus on linearizable data structures, we want to ensure that given
 * some concurrent execution of a set of operations, there exists an equivalent
 * sequential execution of those operations. To this end:
 * 
 * 1. We use Jiffy<Integer, String>, with relatively small number of keys to
 * generate high contention levels.
 * 
 * 2. The tests comprises of a number of worker threads performing a series of
 * operations (put/remove/batch/get/snapshot/scan/higherEntry/lowerEntry/etc.).
 * Each thread logs the operations performed in a so called trace. For each
 * operation we store the operation type, thread id, sequence number, witness
 * values (the value returned by get, values returned by scan (as a map), or
 * values overwritten by put/remove/batch).
 * 
 * 3. All written values (put/batch operations) are unique. Their format is
 * "threadId_seqNum", so by examining the value we can establish which thread
 * performed the update and we can establish the program order of updates.
 * 
 * 4. Naturally all operations performed within a snapshot (so also range scans)
 * are specially marked, so they can be traced to the same snapshot.
 * 
 * 5. Periodically threads obtain the current timestamp (using
 * System.nanotime()) and include it in their log. Obtaining a timestamp is a
 * relatively low-cost operation, so it can be performed frequently (in our
 * tests every 5 operations).
 * 
 * Now we can easily check the traces to establish the following:
 * 
 * 1. If snapshots are atomic - for any key the witness value is always the
 * same.
 * 
 * 2. If there is the monotonic reads guarantee on witness values - for any
 * thread trace and key, the witness value v either is the last recorded value
 * for this key or the value has never been recorded before but its sequence
 * number s is greater than the sequence number s' of the last update operation
 * performed by a thread that also performed s.
 * 
 * 3. If the witness values of update operations are unique - since there must
 * exist a total order of update operation on every key, any written value can
 * be reported exactly once as the witness value for any update operation on the
 * same key for all update operations except the last one (for any key).
 * 
 * Next we proceed to the more in-depth analysis of traces. More precisely, we
 * use the data kept in traces to create a large graph in which nodes correspond
 * to operations and edges correspond to the happens-before relations between
 * operations. Since we evaluate a linearizable data structure, any cycle in the
 * graph indicates a serious problem with the tested implementation.
 * 
 * Initially we include the following edges in the graph: -
 * 
 * 
 * TODO: checker can be made faster - custom path detection that uses timestamps
 * to cut branches
 * 
 * @author Tadeusz Kobus
 *
 */
public class ConcurrentMapTest {

//	private static final Logger logger = LoggerFactory.getLogger(ConcurrentMapTest.class);

	public ConcurrentMapTest() {

	}

	public static class ScenarioConfiguration {
		public float getFrac = 0.25f;
		public float getIterFrac = 0.5f; // pure gets = 1 - getIterFrac
		public int getIterSize = 13;
		public float descendingGetIterFrac = 0.5f;
		public float putDelFrac = 0.25f;
		public float batchFrac = 0.25f; // 0.25f;
		public int batchSize = 6;
		public float snapFrac = 0.25f;

		public int opsPerSnap = 10;
		public float getsPerSnapFrac = 0.5f;
		public float relOpsSnapFrac = 0.25f; // scansProb = 1 - getsPerSnapFrac - relOpsSnapFrac
		public float descendingIterSnapFrac = 0.5f; // regularIterators = 1 - reverseIterSnapFrac;
		public int snapIterSize = 13; // whole iteration counted as an snapshot op

		public float dedicatedSnapshotThreads = 0f;

		public int keys = 10;

		public int operationsPerTimestamp = 5;

		public ScenarioConfiguration() {
		}

		public static ScenarioConfiguration getReducedCheckerNoSnapshotsConfiguration() {
			ScenarioConfiguration conf = new ScenarioConfiguration();

			// getFrac + putDelFrac + conf.batchFrac + snapFrac = 1
			conf.getFrac = 0.25f;
			conf.getIterFrac = 0.1f; // pure gets = 1 - getIterFrac
			conf.getIterSize = 13;
			conf.descendingGetIterFrac = 0.5f;
			conf.putDelFrac = 0.25f;
			conf.batchFrac = 0.25f; // 25f; // 0.25f;
			conf.batchSize = 10; // 4; 5
			conf.snapFrac = 0.25f;

			conf.opsPerSnap = 6;
			conf.getsPerSnapFrac = 0.5f;
			conf.relOpsSnapFrac = 0.01f; // scansProb = 1 - getsPerSnapFrac - relOpsSnapFrac
			conf.descendingIterSnapFrac = 0.3f; // regularIterators = 1 - reverseIterSnapFrac;
			conf.snapIterSize = 50; // whole iteration counted as an snapshot op

			conf.dedicatedSnapshotThreads = 0.5f;// 0.5 // rest = 1 - dedicatedSnapshotThreads
//			conf.dedicatedSnapshotThreads = 0.0f;// 0.5 // rest = 1 - dedicatedSnapshotThreads

			conf.keys = 200;

			conf.operationsPerTimestamp = 5;

			return conf;
		}

		public static ScenarioConfiguration getBatchCheckerConfiguration() {
			ScenarioConfiguration conf = new ScenarioConfiguration();

			conf.getFrac = 0.0f;
			conf.getIterFrac = 0.5f; // pure gets = 1 - getIterFrac
			conf.getIterSize = 13;
			conf.descendingGetIterFrac = 0.5f;
			conf.putDelFrac = 0.0f;
			conf.batchFrac = 1.0f; // 0.25f;
			conf.batchSize = 3;
			conf.snapFrac = 0.0f;

			conf.opsPerSnap = 6;
			conf.getsPerSnapFrac = 1.0f;
			conf.relOpsSnapFrac = 0.0f; // scansProb = 1 - getsPerSnapFrac - relOpsSnapFrac
			conf.descendingIterSnapFrac = 0.0f; // regularIterators = 1 - reverseIterSnapFrac;
			conf.snapIterSize = 10; // whole iteration counted as an snapshot op

			conf.dedicatedSnapshotThreads = 0.0f; // rest = 1 - dedicatedSnapshotThreads

			conf.keys = 10;

			conf.operationsPerTimestamp = 5;

			return conf;
		}

		public static ScenarioConfiguration getPerformanceRunConfiguration() {
			ScenarioConfiguration conf = new ScenarioConfiguration();

			conf.getFrac = 0.25f;
			conf.getIterFrac = 0.0f; // pure gets = 1 - getIterFrac
			conf.getIterSize = 13;
			conf.descendingGetIterFrac = 0.0f;
			conf.putDelFrac = 0.25f;
			conf.batchFrac = 0.25f; // 0.25f;
			conf.batchSize = 6;
			conf.snapFrac = 0.25f;

			conf.opsPerSnap = 2;
			conf.getsPerSnapFrac = 0.7f; // the other fraction are scans
			conf.relOpsSnapFrac = 0.1f; // scansProb = 1 - getsPerSnapFrac - relOpsSnapFrac
			conf.descendingIterSnapFrac = 0.3f; // regularIterators = 1 - reverseIterSnapFrac;
			conf.snapIterSize = 13; // whole iteration counted as an snapshot op

			conf.dedicatedSnapshotThreads = 0f;

			conf.keys = 100000;

			conf.operationsPerTimestamp = 1000000000;

			return conf;
		}

		public ScenarioConfiguration(float getFrac, float getIterFrac, int getIterSize, float descendingGetIterFrac,
				float putDelFrac, float batchFrac, int batchSize, float snapFrac, int opsPerSnap, float getsPerSnapFrac,
				float relOpsSnapFrac, float reverseIterSnapFrac, int snapIterSize, int keys,
				int operationsPerTimestamp) {
			this.getFrac = getFrac;
			this.getIterFrac = getIterFrac; // pure gets = 1 - getIterFrac
			this.getIterSize = getIterSize;
			this.descendingGetIterFrac = descendingGetIterFrac;
			this.putDelFrac = putDelFrac;
			this.batchFrac = batchFrac;
			this.batchSize = batchSize;
			this.snapFrac = snapFrac;
			this.opsPerSnap = opsPerSnap;
			this.getsPerSnapFrac = getsPerSnapFrac;
			this.relOpsSnapFrac = relOpsSnapFrac;
			this.descendingIterSnapFrac = reverseIterSnapFrac;
			this.snapIterSize = snapIterSize;
			this.keys = keys;
			this.operationsPerTimestamp = operationsPerTimestamp;

			assert this.getFrac + this.putDelFrac + this.batchFrac + this.snapFrac == 1;
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();

			builder.append("Settings:\n");
			builder.append(String.format("- getFrac:                %3.2f\n", getFrac));
			builder.append(String.format("- getFrac (pure gets):    %3.2f (1 - getIterFrac)\n", 1 - getIterFrac));
			builder.append(String.format("- getIterFrac:            %3.2f\n", getIterFrac));
			builder.append(String.format("- getIterSize:           %5d\n", getIterSize));
			builder.append(String.format("- descendingGetIterFrac:  %3.2f\n", descendingGetIterFrac));
			builder.append(String.format("- putDelFrac:             %3.2f\n", putDelFrac));
			builder.append(String.format("- batchFrac:              %3.2f\n", batchFrac));
			builder.append(String.format("- batchSize:             %5d\n", batchSize));
			builder.append(String.format("- snapFrac:               %3.2f\n", snapFrac));
			builder.append(String.format("- opsPerSnap:            %5d\n", opsPerSnap));
			builder.append(String.format("- getsPerSnapFrac:        %3.2f\n", getsPerSnapFrac));
			builder.append(String.format("- relOpsSnapFrac:         %3.2f\n", relOpsSnapFrac));
			builder.append(String.format("- iterPerSnapFrac:        %3.2f\n", 1 - getsPerSnapFrac - relOpsSnapFrac));
			builder.append(String.format("- ascIterFrac:            %3.2f\n", 1 - descendingIterSnapFrac));
			builder.append(String.format("- descIterFrac:           %3.2f\n", descendingIterSnapFrac));
			builder.append(String.format("- snapIterSize:          %5d\n", snapIterSize));
			builder.append(String.format(
					"- dedicatedSnapshotTh:    %3.2f (if > 0, relative percentage of ops vs snapshots might be off)\n",
					dedicatedSnapshotThreads));
			builder.append(String.format("- normalThreads:          %3.2f\n", 1 - dedicatedSnapshotThreads));
			builder.append(String.format("- keys:              %9d\n", keys));
			builder.append(String.format("- opsPerTimestamp:%12d\n", operationsPerTimestamp));

			return builder.toString();
		}
	}

	volatile boolean quit = false;

	public List<Trace> run(int mode, int threads, int warmupTime, int execTime, boolean performanceTestsOnly) {
		System.out.format("Running Jiffy concurrent test:\n" + "- mode: %d\n" + "- threads: %d\n"
				+ "- warmup time: %d (millis)\n" + "- exec time: %d (millis)\n\n", mode, threads, warmupTime, execTime);

		run(mode, threads, warmupTime, true, performanceTestsOnly);
		return run(mode, threads, execTime, false, performanceTestsOnly);
	}

	// TESTS_MODE = 0 - default, 1 - short, 2 - not even cycle detection, different
	// convention than in main
	public static int TESTS_MODE = 0;

	private List<Trace> run(int mode, int threads, int durationMillis, boolean dryRun, boolean performanceRun) {
		if (durationMillis == 0)
			return emptyList();

		// different convention than in main
		TESTS_MODE = 2 - mode;

		WatcherThread wt = null;
		boolean watcher = false;

		MultiversionNavigableMap<Integer, String> map;
		if (performanceRun) {
			map = new Jiffy<>(100, 25);
			// map = new Jiffy<>(3, 2);
		} else {
			// map = new HeavyMvSkipListMap<>(3, 2);
			map = new Jiffy<>(10, 3);
			// map = new HeavyMvSkipListMap<>(100, 25);
		}

		ScenarioConfiguration conf;
		if (performanceRun)
			conf = ScenarioConfiguration.getPerformanceRunConfiguration();
		else
			conf = ScenarioConfiguration.getReducedCheckerNoSnapshotsConfiguration();

		Random random = new Random(42);

		System.out.println(conf + "\n");

		if (dryRun && !performanceRun)
			System.out.println("Warming up...");
		else
			System.out.println("Starting...");

		Thread.UncaughtExceptionHandler exceptionHandler = new Thread.UncaughtExceptionHandler() {
			@Override
			public void uncaughtException(Thread th, Throwable ex) {
				System.out.println("Uncaught thread exception: " + ex);
				System.exit(1);
			}
		};

		WorkerThread[] workerThreads = new WorkerThread[threads];
		for (int i = 0; i < threads; i++) {
			workerThreads[i] = new WorkerThread(i, threads, map, conf, random, performanceRun);
			workerThreads[i].setUncaughtExceptionHandler(exceptionHandler);
			workerThreads[i].start();
		}

		if (watcher) {
			wt = new WatcherThread((Jiffy<Integer, String>) map);
			wt.start();
		}

		try {
			Thread.sleep(durationMillis);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		for (int i = 0; i < threads; i++) {
			workerThreads[i].quit();
		}

		if (watcher) {
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			wt.quit();
			try {
				wt.join();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
		}

		for (int i = 0; i < threads; i++) {
			try {
				workerThreads[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		if (dryRun && !performanceRun) {
			System.gc();
			System.gc();
			return emptyList();
		}

		System.out.println(WorkerStatistics.getHeader());
		WorkerStatistics total = new WorkerStatistics(-1);
		for (int i = 0; i < threads; i++) {
			System.out.println(workerThreads[i].statistics);
			total.merge(workerThreads[i].statistics);
		}
		System.out.println(total);

		Jiffy<Integer, String> heavyMap = (Jiffy<Integer, String>) map;
		System.out.format("\nJiffy stats: %s\n", heavyMap.getNodesStatistics());

		RuntimeStatistics totalRuntimeStatistics = new RuntimeStatistics();
		for (int i = 0; i < threads; i++) {
			totalRuntimeStatistics.merge(workerThreads[i].runtimeStatistics);
		}

		if (Jiffy.STATISTICS)
			System.out.println("Runtime statistics:\n" + totalRuntimeStatistics.toString());

		if (performanceRun) {
			return emptyList();
		}

		/**************************************/

		System.out.println();

		return Arrays.asList(workerThreads).stream()
				.map(x -> x.trace)
				.collect(Collectors.toList());
	}
}
