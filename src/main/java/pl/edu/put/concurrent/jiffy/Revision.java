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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.List;

import pl.edu.put.concurrent.MultiversionNavigableMapException;

// Is public to enable debug in ConcTest. Should be nothing

public class Revision<K, V> {
	final static int REGULAR = 0;
	final static int SPLIT = 1;
	final static int MERGE = 2;
	final static int MERGE_TERMINATOR = 4;

	protected MultiVal<K, V> value;
	BatchDescriptor<K, V> descriptor = null;
	int indexOfLeftmostRelevantBatchKey = -1;
	int indexOfRightmostRelevantBatchKey = -1;
	SingleMultiVal.MultiValIndices<K> nextIndices;
	long version = -1;
	Revision<K, V> next;

	double autoscaleParamForReads = 0;
	double autoscaleParamForUpdates = 0;

	public boolean bulk = false;
	public List<Revision<K, V>> bulkRevs;

	protected int type;

	Revision(MultiVal<K, V> value, long version, Revision<K, V> next) {
		this.value = value;
		this.version = version;
		this.next = next;
	}

	Revision(MultiVal<K, V> value, long version, BatchDescriptor<K, V> descriptor, K nodeLeftKey,
			int indexOfLeftmostRelevantBatchKey, int indexOfRightmostRelevantBatchKey, K rightmostRelevantBatchKey,
			Revision<K, V> next, SingleMultiVal.MultiValIndices<K> nextIndices) {
		this.type = REGULAR;

		this.value = value;
		this.version = version;
		this.descriptor = descriptor;
		this.indexOfLeftmostRelevantBatchKey = indexOfLeftmostRelevantBatchKey;
		this.indexOfRightmostRelevantBatchKey = indexOfRightmostRelevantBatchKey;
		this.next = next;
		this.nextIndices = nextIndices;
	}

	public int getType() {
		return type;
	}

	public MultiVal<K, V> getValue() {
		return value;
	}

	public void setValue(MultiVal<K, V> value, SingleMultiVal.MultiValIndices<K> nextIndices) {
		if (this.value != null)
			throw new MultiversionNavigableMapException("The value field is already set.");
		this.value = value;
		this.nextIndices = nextIndices;
	}

	public int size() {
		if (value == null)
			throw new MultiversionNavigableMapException("Should not happen.");
		return value.size();
	}

	protected Revision<K, V> acquireNext() {
		return (Revision<K, V>) NEXT.getAcquire(this);
	}

	protected void nullNext() {
		NEXT.setOpaque(this, null);
	}

	protected void setVersion(long version) {
		VERSION.setOpaque(this, version);
	}

	protected long acquireVersion() {
		return (long) VERSION.getOpaque(this);
	}

	public long effectiveVersion() {
		long version = acquireVersion();
		if (version > 0 || type == MERGE_TERMINATOR || descriptor == null)
			return version;

		version = descriptor.acquireVersion();
		if (version > 0)
			setVersion(version);
		return version;
	}

	protected long trySetVersion(long version) {
		long oldVersion = acquireVersion();
		if (oldVersion > 0)
			return oldVersion;

		long witness = (long) VERSION.compareAndExchange(this, oldVersion, version);
		if (witness == oldVersion)
			return version;
		return witness;
	}

	protected long getVersion() {
		return (long) VERSION.getOpaque(this);
	}

	protected double[] getAutoscaleParam() {
		return new double[] { (double) AUTOSCALE_PARAM_FOR_READS.getOpaque(this),
				(double) AUTOSCALE_PARAM_FOR_UPDATES.getOpaque(this) };
	}

	protected void setAutoscaleParam(double[] param) {
		AUTOSCALE_PARAM_FOR_READS.setOpaque(this, param[0]);
		AUTOSCALE_PARAM_FOR_UPDATES.setOpaque(this, param[1]);
	}

	@Override
	public String toString() {
		if (value == null && version == 0)
			return "TRev";

		// DEBUG
		K nodeLeftKey = null;
		StringBuilder builder = new StringBuilder();
		builder.append(String.format("(%s) Rev%s, ver: %d, desc: %s, leftKey: %s (lBi: %d rBi: %d), val: %s%s", myId(),
				(bulk ? " bulk " + bulk + " " : ""), version,
				(descriptor == null ? "null" : descriptor.acquireVersion()), nodeLeftKey,
				indexOfLeftmostRelevantBatchKey, indexOfRightmostRelevantBatchKey, getValue(),
				(bulk ? ", #bulkrevs: " + bulkRevs + "#" : "")));
		return builder.toString();
	}

	protected String myId() {
		return super.toString();
	}

	// VarHandle mechanics
	private static final VarHandle NEXT;
	private static final VarHandle VERSION;
	private static final VarHandle AUTOSCALE_PARAM_FOR_READS;
	private static final VarHandle AUTOSCALE_PARAM_FOR_UPDATES;

	static {
		try {
			MethodHandles.Lookup l = MethodHandles.lookup();
			NEXT = l.findVarHandle(Revision.class, "next", Revision.class);
			VERSION = l.findVarHandle(Revision.class, "version", long.class);
			AUTOSCALE_PARAM_FOR_READS = l.findVarHandle(Revision.class, "autoscaleParamForReads", double.class);
			AUTOSCALE_PARAM_FOR_UPDATES = l.findVarHandle(Revision.class, "autoscaleParamForUpdates", double.class);

		} catch (ReflectiveOperationException e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	public String listVersion() {
		StringBuilder builder = new StringBuilder();
		builder.append("[ " + version + ", ");
		int i = 0;
		
		while (i < 10) {
			var r = acquireNext();
			if (r == null)
				break;
			builder.append(r.version + ", ");
			i++;
		}
		
		builder.append(" ]");
		return builder.toString();
	}
}