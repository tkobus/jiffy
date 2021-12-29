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

import pl.edu.put.utils.Pair;

public class Node {
	public static Trace.Event ROOT_EVENT = new Trace.TimestampEvent(0);

	Trace.Event event;
	long minTimestamp = 0;
	long maxTimestamp = Long.MAX_VALUE;

	public Node(Trace.Event event) {
		this.event = event;
	}

	// -1 for anything that is not a proper event
	public int getThreadId() {
		if (event == null)
			return -1;
		return event.threadId;
	}

	public int getSeqNum() {
		if (event == null)
			return -1;
		return event.seq;
	}

	public boolean isUpdating() {
		if (event == null)
			return false;
		return event.isUpdating();
	}

	public int[] getUpdatedKeys() {
		if (event == null)
			return null;
		return event.getUpdatedKeys();
	}

	public Pair<Integer, String>[] getWrittenValues() {
		if (event == null)
			return null;
		return event.getWrittenValues();
	}

	public Pair<Integer, String>[] getWitnessValue() {
		if (event == null)
			return null;

		return event.getWitnessValues();
	}

	@Override
	public String toString() {
		String desc = event == null ? "null" : event.toString();
		return String.format("%s [%d/%d]", desc, minTimestamp, maxTimestamp);
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof Node)) {
			return false;
		}
		Node t = (Node) o;

		if (event == null && t.event == null)
			return true;
		if (event != null && t.event == null)
			return false;
		if (event == null && t.event != null)
			return false;
		return event.equals(t.event);
	}
}