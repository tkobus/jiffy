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

public class WorkerStatistics {
	public int threadId;

	public int gets = 0;
	public int puts = 0;
	public int deletes = 0;
	public int batches = 0;
	public int batchPuts = 0;
	public int batchDeletes = 0;
	public int ascendingIters = 0;
	public int descendingIters = 0;
	public int iterGets = 0;
	public int snapshots = 0;
	public int snapshotGets = 0;
	public int snapshotRelOps = 0;
	public int snapshotIters = 0;
	public int snapshotDescendingIters = 0;
	public int snapshotItersGets = 0;
	public long elapsedMilliseconds;

	public int mergeCount = 0;

	public WorkerStatistics(int threadId) {
		this.threadId = threadId;
	}

	public static String getHeader() {
		return "    th |   seconds |      b-op/s |     gets/s     puts/s     dels/s |    btch/s  btch-p/s  btch-d/s |   ascd-i/s   desc-i/s   iter-g/s |    snaps/s   snap-g/s snap-rel/s   snap-i/s  snap-di/s  snap-ig/s |   all-get/s   all-put/s   all-del/s";
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		float seconds = 1f * elapsedMilliseconds / 1000;
		if (mergeCount != 0)
			seconds /= mergeCount;
		builder.append(String.format(
				"%6s | %9.2f | %11.2f | %10.2f %10.2f %10.2f | %9.2f %9.2f %9.2f | %10.2f %10.2f %10.2f | %10.2f %10.2f %10.2f %10.2f %10.2f %10.2f | %11.2f %11.2f %11.2f",
				threadId == -1 ? "TOTAL" : threadId, seconds,
				(gets + puts + deletes + batchPuts + batchDeletes + iterGets + snapshotGets + snapshotItersGets) / seconds,
				gets / seconds, puts / seconds, deletes / seconds, batches / seconds, batchPuts / seconds,
				batchDeletes / seconds, 
				ascendingIters / seconds, descendingIters / seconds, iterGets / seconds,
				snapshots / seconds, snapshotGets / seconds, snapshotRelOps / seconds,
				snapshotIters / seconds, snapshotDescendingIters / seconds,  
				snapshotItersGets / seconds,
				(gets + iterGets + snapshotGets + snapshotItersGets) / seconds, (puts + batchPuts) / seconds,
				(deletes + batchDeletes) / seconds));
		return builder.toString();
	}

	public void merge(WorkerStatistics statistics) {
		this.gets += statistics.gets;
		this.puts += statistics.puts;
		this.deletes += statistics.deletes;
		this.batches += statistics.batches;
		this.batchPuts += statistics.batchPuts;
		this.batchDeletes += statistics.batchDeletes;
		this.ascendingIters += statistics.ascendingIters;
		this.descendingIters += statistics.descendingIters;
		this.iterGets += statistics.iterGets;
		this.snapshots += statistics.snapshots;
		this.snapshotGets += statistics.snapshotGets;
		this.snapshotRelOps += statistics.snapshotRelOps;
		this.snapshotIters += statistics.snapshotIters;
		this.snapshotDescendingIters += statistics.snapshotDescendingIters;
		this.snapshotItersGets += statistics.snapshotItersGets;
		this.elapsedMilliseconds += statistics.elapsedMilliseconds;

		this.mergeCount++;
	}
}
