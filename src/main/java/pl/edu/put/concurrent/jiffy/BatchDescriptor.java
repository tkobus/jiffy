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

class BatchDescriptor<K, V> {

	Batch<K, V> batch;
	private long version = 0;
	private boolean finished = false;

	protected BatchDescriptor(Batch<K, V> batch, long optimisticVersion) {
		this.batch = batch;
		this.version = -optimisticVersion;
	}

	protected long acquireVersion() {
		return (long) VERSION.getOpaque(this);
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

	protected boolean isFinished() {
		return (boolean) FINISHED.getOpaque(this);
	}

	protected void finish() {
		FINISHED.setOpaque(this, true);
	}
	
	public String toString() {
		return String.format("[Descriptor, ver: %d, finished: %s]", version, finished);
	}

	// VarHandle mechanics
	private static final VarHandle FINISHED;
	private static final VarHandle VERSION;

	static {
		try {
			MethodHandles.Lookup l = MethodHandles.lookup();
			FINISHED = l.findVarHandle(BatchDescriptor.class, "finished", boolean.class);
			VERSION = l.findVarHandle(BatchDescriptor.class, "version", long.class);
		} catch (ReflectiveOperationException e) {
			throw new ExceptionInInitializerError(e);
		}
	}
}
