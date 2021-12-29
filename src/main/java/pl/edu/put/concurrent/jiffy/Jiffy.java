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
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import pl.edu.put.concurrent.MultiversionNavigableMap;
import pl.edu.put.concurrent.MultiversionNavigableMapSnapshot;
import pl.edu.put.concurrent.jiffy.SingleMultiVal.MultiValIndices;
import pl.edu.put.utils.Pair;
import pl.edu.put.utils.Quad;
import pl.edu.put.utils.Triple;

public class Jiffy<K, V> extends AbstractMap<K, V> implements MultiversionNavigableMap<K, V> {
	/*
	 * Notation guide for local variables Node: b, n, f, p for predecessor,
	 * node, successor, aux Index: q, r, d for index node, right, down. Head: h
	 * Keys: k, key Values: v, value Comparisons: c
	 */

	@SuppressWarnings("unused")
	private static final long serialVersionUID = -8627078645895051601L;

	enum ScalingMode {
		HardMaxMinSizes(1), MaxMinSizesWithAutoscaling(2), FullAutoscaling(3);

		private int mode;

		ScalingMode(int mode) {
			this.mode = mode;
		}

		public static ScalingMode getMode(int mode) {
			ScalingMode ret = null;
			switch (mode) {
			case 1:
				ret = HardMaxMinSizes;
				break;
			case 2:
				ret = MaxMinSizesWithAutoscaling;
				break;
			case 3:
				ret = FullAutoscaling;
				break;
			}

			return ret;
		}

		@Override
		public String toString() {
			String ret = null;

			switch (mode) {
			case 1:
				ret = "HardMaxMinSizes";
				break;
			case 2:
				ret = "MaxMinSizesWithAutoscaling";
				break;
			case 3:
				ret = "FullAutoscaling";
				break;
			}

			return ret;
		}
	}

	volatile ScalingMode NODE_SCALING_MODE = ScalingMode.HardMaxMinSizes;

	int MAX_MULTIVAL_SIZE = 3;
	volatile int MIN_MULTIVAL_SIZE = 2;

	volatile NodeAutoScaleConfiguration AUTOSCALE_CONFIGURATION = new NodeAutoScaleConfiguration();

	// Should be false by default, as adds an overhead
	public static boolean STATISTICS = false;

	// Should be true for interactive tests
	public static boolean SPLIT_MERGE_STATISTICS = true;

	public static boolean USE_TSC = true;

	ThreadLocal<RuntimeStatistics> stats = new ThreadLocal<>() {
		protected RuntimeStatistics initialValue() {
			return new RuntimeStatistics();
		};
	};

	ThreadLocal<Long> splitCounter = new ThreadLocal<>() {
		protected Long initialValue() {
			return 0l;
		};
	};

	ThreadLocal<Long> mergeCounter = new ThreadLocal<>() {
		protected Long initialValue() {
			return 0l;
		};
	};

	public long getSplitCount() {
		return splitCounter.get();
	}

	public long getMergeCount() {
		return mergeCounter.get();
	}

	public long[] getBloomCounterStats() {
		return SingleMultiVal.getBloomStats();
	}

	public void setNodeSizes(int maxNodeSize, int minNodeSize) {
		this.MAX_MULTIVAL_SIZE = maxNodeSize;
		this.MIN_MULTIVAL_SIZE = minNodeSize;
	}

	public void setNodeScalingMode(int mode) {
		this.NODE_SCALING_MODE = ScalingMode.getMode(mode);
	}

	public void setNodeAutoScaleConfiguration(NodeAutoScaleConfiguration autoscaleConfiguration) {
		this.AUTOSCALE_CONFIGURATION = autoscaleConfiguration;
	}

	public RuntimeStatistics getRuntimeStatistics() {
		return stats.get();
	}

	public Pair<Integer, List<Integer>> getStructureStatistics(int detailedStatsMaxDepth) {
		Index<K, V> h;
		int indexHeight = 0;
		List<Integer> levelCountArray = new ArrayList<>();
		VarHandle.acquireFence();
		h = head;
		for (Index<K, V> q = h, d;;) { // count while descending
			indexHeight++;
			if (indexHeight <= detailedStatsMaxDepth) {
				int levelCount = 1;
				Index<K, V> r = q;
				while ((r = r.right) != null) {
					levelCount++;
				}
				levelCountArray.add(levelCount);
			}
			if ((d = q.down) != null)
				q = d;
			else {
				// b = q.node;
				break;
			}
		}

		return new Pair<Integer, List<Integer>>(indexHeight, levelCountArray);
	}

	ThreadLocal<Integer> currentReadOpAutoscalerSkip = new ThreadLocal<>() {
		protected Integer initialValue() {
			return 0;
		};
	};

	ThreadLocal<Integer> lastReadOpAutoscalerSkip = new ThreadLocal<>() {
		protected Integer initialValue() {
			return 0;
		};
	};

	ThreadLocal<Long> autoscalingLastTimeSet = new ThreadLocal<>() {
		protected Long initialValue() {
			return getCurrentVersion();
		};
	};

	/**
	 * The comparator used to maintain order in this map, or null if using natural
	 * ordering. (Non-private to simplify access in nested classes.)
	 * 
	 * @serial
	 */
	final Comparator<? super K> comparator;

	/** Lazily initialized topmost index of the skiplist. */
	private transient Index<K, V> head;

	@SuppressWarnings("unused")
	private long currentVersion = 0;
	@SuppressWarnings("unused")
	private SubMap<K, V> snapshots = null;
	
	/* ---------------- Utilities -------------- */

	/**
	 * Compares using comparator or natural ordering if null. Called only by methods
	 * that have performed required type checks.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	static int cpr(Comparator c, Object x, Object y) {
		if (y == null)
			return 1;
		return (c != null) ? c.compare(x, y) : ((Comparable<Object>) x).compareTo(y);
	}

	/**
	 * Returns the header for base node list, or null if uninitialized
	 */
	final Node<K, V> baseHead() {
		Index<K, V> h;
		VarHandle.acquireFence();
		return ((h = head) == null) ? null : h.node;
	}

	private void initializeHead() {
		VarHandle.acquireFence();

		if (head != null) // nothing to do
			return;

		long finalVersion = 1; // currentVersion == 0;
		Node<K, V> base = new Node<K, V>(null, null, null, null);
		MultiVal<K, V> mval = new SingleMultiVal<>(true);
		base.revisionHead = new Revision<K, V>(mval, finalVersion, null, null, -1, -1, null, null, null);
		Index<K, V> h = new Index<K, V>(base, null, null);
		HEAD.compareAndSet(this, null, h);
		publishVersion(finalVersion);
	}

	/**
	 * Tries to unlink deleted node n from predecessor b (if both exist), by first
	 * splicing in a marker if not already present. Upon return, node n is sure to
	 * be unlinked from b, possibly via the actions of some other thread.
	 *
	 * @param b if nonnull, predecessor
	 * @param n if nonnull, node known to be deleted
	 */

	static <K, V> void unlinkNode(Node<K, V> b, Node<K, V> n) {
		if (b != null && n != null) {
			Node<K, V> p = n.next;
			NEXT.compareAndSet(b, n, p);
		}
	}

	/* ---------------- Traversal -------------- */

	/**
	 * Returns an index node with key strictly less than given key. Also unlinks
	 * indexes to deleted nodes found along the way. Callers rely on this
	 * side-effect of clearing indices to deleted nodes.
	 *
	 * @param key if nonnull the key
	 * @return a predecessor node of key, or null if uninitialized or null key
	 */
	private Node<K, V> findPredecessor(Object key, Comparator<? super K> cmp) {
		Index<K, V> q;
		VarHandle.acquireFence();
		if (key == null)
			return null;

		q = head;
		for (Index<K, V> r, d;;) {
			while ((r = q.right) != null) {
				Node<K, V> p = r.node;
				K k = p != null ? p.key : null;
				if (p == null || p.isTerminated()) // unlink index to deleted node
					RIGHT.compareAndSet(q, r, r.right);
				else if (cpr(cmp, key, k) > 0)
					q = r;
				else // the key of current node is greater or equal 'key', we've gone too far
					break;
			}
			if ((d = q.down) != null) {
				q = d;
			} else
				return q.node;
		}
	}

	/**
	 * Gets value for key.
	 *
	 * @param key the key
	 * @return the value, or null if absent
	 */
	@SuppressWarnings("unchecked")
	private V doGet(Object key, long version) {
		if (key == null)
			throw new NullPointerException();

		long[] statsArray = null;
		if (STATISTICS)
			statsArray = new long[5];

		Node<K, V> ret = null;
		Revision<K, V> retHead = null;
		Comparator<? super K> cmp = comparator;
		outer: for (;;) {
			if (STATISTICS)
				statsArray[0]++;
			Index<K, V> h;
			Node<K, V> b;
			VarHandle.acquireFence();
			h = head;
			for (Index<K, V> q = h, r, d;;) { // count while descending
				while ((r = q.right) != null) {
					Node<K, V> p = r.node;
					K k = p != null ? p.key : null;
					if (p == null || p.isTerminated())
						RIGHT.compareAndSet(q, r, r.right);
					else if (cpr(cmp, key, k) > 0)
						q = r;
					else
						break;
				}
				if ((d = q.down) != null)
					q = d;
				else {
					b = q.node;
					break;
				}
			}

			assert b != null; // because we have at least one MultiVal
			Node<K, V> n;
			insertionPoint: for (;;) { // find insertion point
				if (STATISTICS)
					statsArray[1]++;
				// K k;
				int c;
				n = b.next;
				K k = n != null ? n.key : null;
				if (n == null) {
					if (b.key == null) // if empty, type check key now
						cpr(cmp, key, key);
					c = -1;
				} else if (n.isTerminated()) {
					unlinkNode(b, n);
					c = 1;
				} else if ((c = cpr(cmp, key, k)) >= 0)
					b = n;

				if (c < 0) {
					if (b.getType() == Node.TEMP_SPLIT) {
						if (STATISTICS)
							statsArray[2]++;
						retHead = ((TempSplitNode<K, V>) b).leftRevision;
						ret = ((SplitRevision<K, V>) retHead).node;
						if (retHead.effectiveVersion() > 0) {
							// we may have observed a faulty TempSplitNode introduced through ABA
							if (ret.acquireNext() == b) {
								b.terminate();
								unlinkNode(ret, b);
							}
							continue outer;
						}
						break outer;
					}

					Revision<K, V> head = b.acquireRevisionHead();
					if (head.getType() == Revision.MERGE_TERMINATOR) {
						if (STATISTICS)
							statsArray[3]++;
						helpMergeTerminator((MergeTerminatorRevision<K, V>) head);
						cleanTerminatedNode(b.key);
						continue outer;
					}

					if (b.acquireNext() != n) {
						if (STATISTICS)
							statsArray[4]++;
						continue insertionPoint;
					}

					ret = b;
					retHead = head;
					break outer;
				}
			}
		}
		assert ret != null; // because we always have at least one MultiVal (with or without the key)

		if (STATISTICS)
			stats.get().updateDoGet(statsArray);

		return getProperValue((K) key, ret, retHead, version);
	}

	static ThreadLocal<Integer> dotCounter = new ThreadLocal<>() {
		protected Integer initialValue() {
			return 0;
		};
	};

	private Revision<K, V> retrieveRevision(Node<K, V> node, K key, long version, Revision<K, V> head) {
		int skip = currentReadOpAutoscalerSkip.get();

		if (skip == 0) {
			double[] autoscaleParam = head.getAutoscaleParam();

			long lastSetTime = autoscalingLastTimeSet.get();
			long currentTime = getCurrentVersion();
			autoscalingLastTimeSet.set(currentTime);

			head.setAutoscaleParam(newAutoscaleParamForReads(autoscaleParam, currentTime - lastSetTime));

			skip = AUTOSCALE_CONFIGURATION.readOpMaxSkip;
			currentReadOpAutoscalerSkip.set(skip);
			lastReadOpAutoscalerSkip.set(skip);
		} else {
			currentReadOpAutoscalerSkip.set(skip - 1);
		}

		Revision<K, V> revision;

		long[] statsArray = null;
		if (STATISTICS)
			statsArray = new long[3];

		if (version == NEWEST_VERSION) {
			revision = node.getNewestRevision(key, head, this, statsArray);

			if (STATISTICS)
				stats.get().updateGetNewestRevision(statsArray);

		} else {
			revision = node.getRevision(key, version, head, this, statsArray);

			if (STATISTICS) {
				stats.get().updateGetRevision(statsArray);

				if (revision != null && revision.bulk) {
					stats.get().updateGetRevisionBulkOnly(new long[] { revision.bulkRevs.size() });
				}
			}
		}

		return revision;
	}

	private Revision<K, V> retrieveBatchRevision(Node<K, V> node, K key, BatchDescriptor<K, V> descriptor,
			Revision<K, V> head) {
		Revision<K, V> revision;

		long[] statsArray = null;
		if (STATISTICS)
			statsArray = new long[3];

		revision = node.getBatchRevision(key, descriptor, head, this, statsArray);

		if (STATISTICS)
			stats.get().updateGetRevision(statsArray);

		return revision;
	}

	private V getProperValue(K key, Node<K, V> node, Revision<K, V> head, long version) {
		Revision<K, V> revision = retrieveRevision(node, key, version, head);
		return revision == null ? null : revision.getValue().get(key);
	}

	@SuppressWarnings("unchecked")
	private boolean containsValue(Node<K, V> node, long version, Object value) {
		Revision<K, V> revision = retrieveRevision(node, null, version, null);

		return revision != null && revision.getValue().containsValue((V) value);
	}

	/* ---------------- Insertion -------------- */

	/**
	 * Added to replace the nextSecondarySeed from the internal java.util.concurrent
	 * threadLocal.
	 **/
	static class ThreadLocalRandom {
		static ThreadLocal<Random> tlr = ThreadLocal.withInitial(() -> new Random());

		static final int nextSecondarySeed() {
			return tlr.get().nextInt();
		}
	}

	private double[] newAutoscaleParamForUpdates(double[] currentAutoscalerParams, long delta) {
		double deltaSeconds = Math.min(delta / 10000000.0, 0.5);
		double[] ret = new double[] { (1 - deltaSeconds) * currentAutoscalerParams[0],
				deltaSeconds + (1 - deltaSeconds) * currentAutoscalerParams[1] };

		return ret;
	}

	private double[] newAutoscaleParamForReads(double[] currentAutoscalerParams, long delta) {
		double deltaSeconds = Math.min(delta / 10000000.0, 0.5);
		double[] ret = new double[] { deltaSeconds + (1 - deltaSeconds) * currentAutoscalerParams[0],
				(1 - deltaSeconds) * currentAutoscalerParams[1] };

		return ret;
	}

	private int whatUpdate(int endSize, double[] newAutoscaleParams, Revision<K, V> head) {
		int ret = 0;

		switch (NODE_SCALING_MODE) {
		case HardMaxMinSizes: {
			if (endSize > MAX_MULTIVAL_SIZE)
				ret = 1;
			else if (endSize < MIN_MULTIVAL_SIZE)
				ret = -1;
			break;
		}
		case MaxMinSizesWithAutoscaling: {
//			if (endSize > MAX_MULTIVAL_SIZE
//					|| (endSize >= 2 && newAutoscaleParam > AUTOSCALE_CONFIGURATION.splitThreshold))
//				ret = 1;
//			else if (endSize <= MIN_MULTIVAL_SIZE || newAutoscaleParam < AUTOSCALE_CONFIGURATION.mergeThreshold)
//				ret = -1;
			break;
		}
		case FullAutoscaling: {
			// 0.25 writers - prefSize should be 100
			// 0.5 writers - prefSize should be 50
			// y = ax + b
			// b = 150
			// a = -200

			double sum = newAutoscaleParams[0] + newAutoscaleParams[1];
			double writersRatio = sum == 0 ? 0.25 : newAutoscaleParams[1] / sum;

			// int preferredSize = writersRatio > 0.375 ? 50 : 100;
			int preferredSize = (int) Math.max(-200 * writersRatio + 150, 25);
			if (endSize >= 2 && endSize > 3 * preferredSize)
				ret = 1;
			else if (endSize < preferredSize * 2 / 3) {
				if (endSize < Short.MAX_VALUE / 2)
					ret = -1;
			}

			break;
		}
		default: {
			throw new JiffyInternalException("wrong scaling mode, should not happen");
		}
		}

		return ret;
	}

	/**
	 * Main insertion method for single puts. Adds element if not present, or
	 * replaces value if present and onlyIfAbsent is false.
	 *
	 * @param key          the key
	 * @param value        the value that must be associated with key
	 * @param onlyIfAbsent if should not insert if already present
	 * @return the newly inserted revision and the next revision
	 */
	private Triple<Revision<K, V>, Revision<K, V>, Integer> doPutSingle(K key, V value, long optimisticVersion,
			boolean doRemove) {
		if (head != null && key == null)
			throw new NullPointerException();

		Triple<Revision<K, V>, Revision<K, V>, Integer> ret = null;
		long[] statsArray = null;
		if (STATISTICS)
			statsArray = new long[9];

		Comparator<? super K> cmp = comparator;
		Revision<K, V> revision = new Revision<K, V>(null, -optimisticVersion, null);

		outer: for (;;) {
			if (STATISTICS)
				statsArray[0]++;
			Index<K, V> h;
			Node<K, V> b;
			VarHandle.acquireFence();
			int levels = 0; // number of levels descended
			h = head;
			for (Index<K, V> q = h, r, d;;) { // count while descending
				while ((r = q.right) != null) {
					Node<K, V> p = r.node;
					K k = p != null ? p.key : null;
					if (p == null || p.isTerminated())
						RIGHT.compareAndSet(q, r, r.right);
					else if (cpr(cmp, key, k) > 0)
						q = r;
					else
						break;
				}
				if ((d = q.down) != null) {
					++levels;
					q = d;
				} else {
					b = q.node;
					break;
				}
			}

			assert b != null; // because we have at least one MultiVal
			Node<K, V> n;
			insertionPoint: for (;;) { // find insertion point
				if (STATISTICS)
					statsArray[1]++;
				int c;
				n = b.next;
				K k = n != null ? n.key : null;
				if (n == null) {
					if (b.key == null) // if empty, type check key now
						cpr(cmp, key, key);
					c = -1;
				} else if (n.isTerminated()) {
					unlinkNode(b, n);
					c = 1;
				} else if ((c = cpr(cmp, key, k)) >= 0)
					b = n;

				if (c < 0) {
					if (b.getType() == Node.TEMP_SPLIT) {
						if (STATISTICS)
							statsArray[2]++;
						helpTempSplitNode((TempSplitNode<K, V>) b, null);
						continue outer;
					}

					Revision<K, V> head = b.acquireRevisionHead();

					if (b.isTerminated()) {
						if (STATISTICS)
							statsArray[3]++;
						continue outer; // retry
					}

					if (head.effectiveVersion() < 0
							&& (head.descriptor != null || head.getType() != Revision.REGULAR)) {
						if (STATISTICS)
							statsArray[4]++;
						helpPut(head);
						continue insertionPoint;
					}

					if (b.acquireNext() != n) {
						if (STATISTICS)
							statsArray[5]++;
						continue insertionPoint;
					}

					MultiVal<K, V> headMval = head.getValue();
					int index = headMval.indexOfKeyInMultiVal(key);

					long delta = optimisticVersion - autoscalingLastTimeSet.get();
					double[] newAutoscaleParam = newAutoscaleParamForUpdates(head.getAutoscaleParam(), delta);

					int endSize = headMval.size() + (index >= 0 ? 0 : 1);
					if (whatUpdate(endSize, newAutoscaleParam, head) != 1) {
						if (STATISTICS)
							statsArray[6]++;
						MultiVal<K, V> mval;
						if (!doRemove)
							mval = headMval.add(key, value, index);
						else
							mval = headMval.remove(key, index);
						revision.value = null;
						revision.setValue(mval, null);
						revision.next = head;
						revision.setAutoscaleParam(newAutoscaleParam);

						if (b.tryPutRevisionSingle(revision)) {
							ret = new Triple<>(revision, head, index);
							break outer;
						}
						continue insertionPoint;
					} else {
						if (STATISTICS)
							statsArray[7]++;

						if (head.effectiveVersion() < 0) {
							if (STATISTICS)
								statsArray[8]++;
							helpPut(head);

							// optimization
							Revision<K, V> newHead = b.acquireRevisionHead();
							if (newHead != head)
								continue insertionPoint;
						}

						Pair<MultiVal<K, V>, MultiVal<K, V>> mvalPair = head.getValue().addAndSplit(key, value, index);
						SplitRevision<K, V> leftRevision = new SplitRevision<>(mvalPair.first, revision.version, head,
								true);
						SplitRevision<K, V> rightRevision = new SplitRevision<>(mvalPair.second, revision.version, head,
								false);
						leftRevision.sibling = rightRevision;
						rightRevision.sibling = leftRevision;

						leftRevision.setAutoscaleParam(newAutoscaleParam);
						rightRevision.setAutoscaleParam(newAutoscaleParam);

						leftRevision.node = b;
						leftRevision.levels = levels;

						if (!b.tryPutRevisionSingle(leftRevision))
							continue insertionPoint; // retry

						if (SPLIT_MERGE_STATISTICS)
							splitCounter.set(splitCounter.get() + 1);

						revision = cpr(comparator, key, rightRevision.getValue().firstKey()) < 0 ? leftRevision
								: rightRevision;

						helpSplit(leftRevision);

						ret = new Triple<>(revision, revision.next, index);
						break outer;
					}
				}
			}
		}

		autoscalingLastTimeSet.set(optimisticVersion);

		if (STATISTICS)
			stats.get().updateDoPutSingle(statsArray);

		return ret;
	}

	// TODO find uses, maybe avoid if
	private long helpTempSplitNode(TempSplitNode<K, V> tempSplitNode, Node<K, V> previous) {
		long ret = 0;

		if (tempSplitNode.leftRevision.descriptor == null)
			ret = helpTempSplitNodeSingle(tempSplitNode, previous);
		else
			ret = helpTempSplitNodeBatch(tempSplitNode, previous);

		return ret;
	}

	private long helpTempSplitNodeSingle(TempSplitNode<K, V> tempSplitNode, Node<K, V> previous) {
		long ret;
		long[] statsArray = null;
		if (STATISTICS)
			statsArray = new long[2];

		long version = tempSplitNode.leftRevision.acquireVersion();
		if (version > 0) {
			if (STATISTICS)
				statsArray[0]++;
			tempSplitNode.terminate();
			if (previous != null)
				unlinkNode(previous, tempSplitNode);
			else {
				if (STATISTICS)
					statsArray[1]++;
				cleanTerminatedNode(tempSplitNode.key);
			}
			ret = version;
		} else {
			ret = helpPut(tempSplitNode.leftRevision);
		}

		if (STATISTICS)
			stats.get().updateHelpTempSplitNode(statsArray);
		return ret;
	}

	// Almost the same as helpTempSplitNodeSingle, keeping separate for clarity
	private long helpTempSplitNodeBatch(TempSplitNode<K, V> tempSplitNode, Node<K, V> previous) {
		long ret;
		long[] statsArray = null;
		if (STATISTICS)
			statsArray = new long[2];

		long version = tempSplitNode.leftRevision.effectiveVersion();
		if (version > 0) {
			if (STATISTICS)
				statsArray[0]++;
			tempSplitNode.terminate();
			if (previous != null)
				unlinkNode(previous, tempSplitNode);
			else {
				if (STATISTICS)
					statsArray[1]++;
				cleanTerminatedNode(tempSplitNode.key);
			}
			ret = version;
		} else {
			ret = helpPut(tempSplitNode.leftRevision);
		}

		if (STATISTICS)
			stats.get().updateHelpTempSplitNodeBatch(statsArray);
		return ret;
	}

	// TODO find uses, maybe avoid if
	private long helpSplit(SplitRevision<K, V> revision) {
		long ret = 0;

		if (revision.descriptor == null)
			ret = helpSplitSingle(revision);
		else
			ret = helpSplitBatch(revision);

		return ret;
	}

	private long helpSplitSingle(SplitRevision<K, V> revision) {
		long[] statsArray = null;
		if (STATISTICS)
			statsArray = new long[11];

		long ret = 0;

		outerIf: if (revision.left) {
			if (STATISTICS)
				statsArray[0]++;

			SplitRevision<K, V> leftRevision = revision;
			SplitRevision<K, V> rightRevision = leftRevision.sibling;

			Node<K, V> b = leftRevision.node;
			TempSplitNode<K, V> s = null;

			outer: while (true) {
				if (STATISTICS)
					statsArray[1]++;

				Revision<K, V> hh = b.acquireRevisionHead();

				if (b.isTerminated()) {
					if (STATISTICS)
						statsArray[2]++;

					ret = revision.acquireVersion();
					break outerIf;
				}

				long ver = hh.effectiveVersion();
				if (ver > 0) {
					if (STATISTICS)
						statsArray[3]++;

					ret = ver;
					break outerIf;
				}

				while (true) {
					if (STATISTICS)
						statsArray[4]++;

					Node<K, V> n = b.acquireNext();
					if (n != null && n.acquireRevisionHead() == rightRevision) {
						if (STATISTICS)
							statsArray[5]++;

						long version = leftRevision.acquireVersion();
						if (version < 0) {
							long currentVersion = getCurrentVersion();
							if (currentVersion < -version)
								publishVersion(currentVersion);
							version = leftRevision.trySetVersion(currentVersion);
						}

						rightRevision.trySetVersion(version);
						ret = version;
						break outerIf;
					}

					long version = leftRevision.acquireVersion();
					if (version > 0) {
						if (STATISTICS)
							statsArray[6]++;

						ret = rightRevision.trySetVersion(version);
						break outerIf;
					}

					if (n != null && n.getType() == Node.TEMP_SPLIT) {
						if (STATISTICS)
							statsArray[7]++;

						s = (TempSplitNode<K, V>) n;
						break;
					}

					if (s == null)
						s = new TempSplitNode<>(rightRevision.getValue().firstKey(), leftRevision, n);
					else
						s.next = n;

					if (NEXT.compareAndSet(b, n, s)) {
						if (STATISTICS)
							statsArray[8]++;

						break;
					}
				}

				long version = leftRevision.acquireVersion();
				if (version > 0) {
					if (STATISTICS)
						statsArray[9]++;

					// fixes the ABA problem - thread reads next, goes to sleep, someone else
					// finishes whole split, node gets removed, and the thread wakes up
					if (b.acquireNext() == s) {
						s.terminate();
						unlinkNode(b, s);
					}

					ret = rightRevision.trySetVersion(version);
					break outerIf;
				}

				Node<K, V> p = new Node<>(rightRevision.getValue().firstKey(), rightRevision, s.next, comparator);

				if (!NEXT.compareAndSet(b, s, p))
					continue outer; // p = null;

				if (STATISTICS)
					statsArray[10]++;

				long currentVersion = getCurrentVersion();
				if (currentVersion < -version)
					publishVersion(currentVersion);
				version = leftRevision.trySetVersion(currentVersion);

				rightRevision.trySetVersion(version);

				if (p != null) {
					int levels = leftRevision.levels;
					VarHandle.acquireFence();
					Index<K, V> h = head;
					int lr = ThreadLocalRandom.nextSecondarySeed();
					if ((lr & 0x3) == 0) { // add indices with 1/4 prob
						int hr = ThreadLocalRandom.nextSecondarySeed();
						long rnd = ((long) hr << 32) | ((long) lr & 0xffffffffL);
						int skips = levels; // levels to descend before add
						Index<K, V> x = null;
						for (;;) { // create at most 62 indices
							x = new Index<K, V>(p, x, null);
							if (rnd >= 0L || --skips < 0)
								break;
							else
								rnd <<= 1;
						}
						if (addIndices(h, skips, x, comparator) && skips < 0 && head == h) { // try to add new
																								// level
							Index<K, V> hx = new Index<K, V>(p, x, null);
							Index<K, V> nh = new Index<K, V>(h.node, h, hx);
							HEAD.compareAndSet(this, h, nh);
						}
						if (p.isTerminated()) // deleted while adding indices
							findPredecessor(p.key, comparator); // clean
					}
				}

				ret = version;
				break outerIf;
			}
		} else {
			SplitRevision<K, V> rightSibling = revision;
			SplitRevision<K, V> leftSibling = revision.sibling;

			// Since we're helping rightSibling, it's node must have been already CASsed-in.
			// Hence we need to make sure that the versions are set.
			long version = leftSibling.acquireVersion();
			if (version < 0) {
				long currentVersion = getCurrentVersion();
				if (currentVersion < -version)
					publishVersion(currentVersion);
				version = leftSibling.trySetVersion(currentVersion);
			}
			ret = rightSibling.trySetVersion(version);
		}

		if (STATISTICS)
			stats.get().updateHelpSplit(statsArray);
		return ret;
	}

	private long helpSplitBatch(SplitRevision<K, V> revision) {
		long[] statsArray = null;
		if (STATISTICS)
			statsArray = new long[11];

		long ret = 0;

		outerIf: if (revision.left) {
			if (STATISTICS)
				statsArray[0]++;

			SplitRevision<K, V> leftRevision = revision;
			SplitRevision<K, V> rightRevision = leftRevision.sibling;

			Node<K, V> b = leftRevision.node;
			TempSplitNode<K, V> s = null;

			outer: while (true) {
				if (STATISTICS)
					statsArray[1]++;

				Revision<K, V> hh = b.acquireRevisionHead();

				if (b.isTerminated()) {
					if (STATISTICS)
						statsArray[2]++;

					ret = leftRevision.effectiveVersion();
					break outerIf;
				}

				long ver = hh.effectiveVersion();
				if (ver > 0) {
					if (STATISTICS)
						statsArray[3]++;

					ret = ver;
					break outerIf;
				}

				while (true) {
					if (STATISTICS)
						statsArray[4]++;

					Node<K, V> n = b.acquireNext();
					if (n != null && n.acquireRevisionHead() == rightRevision) {
						if (STATISTICS)
							statsArray[5]++;

						long version = leftRevision.effectiveVersion();
						ret = version;
						break outerIf;
					}

					long version = leftRevision.effectiveVersion();
					if (version > 0) {
						if (STATISTICS)
							statsArray[6]++;

						ret = rightRevision.trySetVersion(version);
						break outerIf;
					}

					if (n != null && n.getType() == Node.TEMP_SPLIT) {
						if (STATISTICS)
							statsArray[7]++;

						s = (TempSplitNode<K, V>) n;
						break;
					}

					if (s == null)
						s = new TempSplitNode<>(rightRevision.getValue().firstKey(), leftRevision, n);
					else
						s.next = n;

					if (NEXT.compareAndSet(b, n, s)) {
						if (STATISTICS)
							statsArray[8]++;

						break;
					}
				}

				long version = leftRevision.effectiveVersion();
				if (version > 0) {
					if (STATISTICS)
						statsArray[9]++;

					// fixes the ABA problem - thread reads next, goes to sleep, someone else
					// finishes whole split, node gets removed, and the thread wakes up
					if (b.acquireNext() == s) {
						s.terminate();
						unlinkNode(b, s);
					}

					ret = rightRevision.trySetVersion(version);
					break outerIf;
				}

				Node<K, V> p = new Node<>(rightRevision.getValue().firstKey(), rightRevision, s.next, comparator);

				if (!NEXT.compareAndSet(b, s, p))
					continue outer; // p = null;

				if (STATISTICS)
					statsArray[10]++;

				if (p != null) {
					int levels = leftRevision.levels;
					VarHandle.acquireFence();
					Index<K, V> h = head;
					int lr = ThreadLocalRandom.nextSecondarySeed();
					if ((lr & 0x3) == 0) { // add indices with 1/4 prob
						int hr = ThreadLocalRandom.nextSecondarySeed();
						long rnd = ((long) hr << 32) | ((long) lr & 0xffffffffL);
						int skips = levels; // levels to descend before add
						Index<K, V> x = null;
						for (;;) { // create at most 62 indices
							x = new Index<K, V>(p, x, null);
							if (rnd >= 0L || --skips < 0)
								break;
							else
								rnd <<= 1;
						}
						if (addIndices(h, skips, x, comparator) && skips < 0 && head == h) { // try to add new
																								// level
							Index<K, V> hx = new Index<K, V>(p, x, null);
							Index<K, V> nh = new Index<K, V>(h.node, h, hx);
							HEAD.compareAndSet(this, h, nh);
						}
						if (p.isTerminated()) // deleted while adding indices
							findPredecessor(p.key, comparator); // clean
					}
				}
				ret = version;
				break outerIf;
			}
		} else {
			// Since we're helping rightSibling, it's node must have been already cassed-in.
			// Hence, we don't have anything to do here.

			ret = revision.effectiveVersion();
		}

		if (STATISTICS)
			stats.get().updateHelpSplitBatch(statsArray);

		return ret;
	}

	// TODO find uses, maybe avoid if
	private long helpMerge(MergeRevision<K, V> mergeRevision) {
		long ret;

		if (mergeRevision.descriptor == null) 
			ret = helpMergeSingle(mergeRevision);
		else 
			ret = helpMergeBatch(mergeRevision);
		
		return ret;
	}

	private long helpMergeSingle(MergeRevision<K, V> mergeRevision) {
		long[] statsArray = null;
		if (STATISTICS)
			statsArray = new long[1];
		
		long ret;
		long version = mergeRevision.acquireVersion();

		if (version > 0) {
			if (STATISTICS)
				statsArray[0]++;

			ret = version;
		} else {
			mergeRevision.nodeToTerminate.debugMergeRev = mergeRevision;

			Node<K, V> nodeToTerminate = mergeRevision.nodeToTerminate;
			if (!nodeToTerminate.isTerminated())
				nodeToTerminate.terminate();

			cleanTerminatedNode(nodeToTerminate.key);
			tryReduceLevel();

			long currentVersion = getCurrentVersion();
			if (currentVersion < -version)
				publishVersion(currentVersion);
			version = mergeRevision.trySetVersion(currentVersion);

			ret = version;
		}

		if (STATISTICS)
			stats.get().updateHelpMerge(statsArray);
		return ret;
	}

	private long helpMergeBatch(MergeRevision<K, V> mergeRevision) {
		long ret;
		long[] statsArray = null;
		if (STATISTICS)
			statsArray = new long[1];
		
		long version = mergeRevision.effectiveVersion();

		if (version > 0) {
			if (STATISTICS)
				statsArray[0]++;

			ret = version;
		} else {
			mergeRevision.nodeToTerminate.debugMergeRev = mergeRevision;

			Node<K, V> nodeToTerminate = mergeRevision.nodeToTerminate;
			if (!nodeToTerminate.isTerminated())
				nodeToTerminate.terminate();

			cleanTerminatedNode(nodeToTerminate.key);
			tryReduceLevel();

			ret = version;
		}

		if (STATISTICS)
			stats.get().updateHelpMergeBatch(statsArray);
		return ret;
	}

	// TODO find uses, maybe avoid if
	private long helpMergeTerminator(MergeTerminatorRevision<K, V> terminator) {
		long ret = 0;

		if (terminator.descriptor == null) 
			ret = helpMergeTerminatorSingle(terminator, ret);
		else
			ret = helpMergeTerminatorBatch(terminator);
		
		return ret;
	}

	private long helpMergeTerminatorSingle(MergeTerminatorRevision<K, V> terminator, long ret) {
		long[] statsArray = null;
		if (STATISTICS)
			statsArray = new long[15];
		
		Node<K, V> b = terminator.node;
		Node<K, V> bb = null;

		outer: while (true) {
			if (STATISTICS)
				statsArray[0]++;

			if (b.isTerminated()) {
				if (STATISTICS)
					statsArray[1]++;
				
				cleanTerminatedNode(b.key);
				tryReduceLevel();
				
				ret = terminator.acquireVersion();
				break outer;
			}

			Node<K, V> bbb = null;
			Node<K, V> nb = null; // new b
			if (bb == null) {
				if (STATISTICS)
					statsArray[2]++;
				
				bb = findPredecessor(b.key, comparator);
			}

			while (true) {
				if (STATISTICS)
					statsArray[3]++;

				// b is already unlinked
				// bb == null probably never happens
				if (bb == null || (bb.key != null && cpr(comparator, bb.key, b.key) > 0)) {
					if (STATISTICS)
						statsArray[4]++;

					if (!b.isTerminated()) {
						if (STATISTICS)
							statsArray[5]++;

						// Hypothesis: in ascend in the iterator, if we enter a node with mergeTerminator
						// we help and then perform entirely new findNear(GT) to get to the next node
						// because the .next reference of the node can be broken. Perhaps the same 
						// issue here.
						bb = null;
						continue outer;
					}

					// hack, because we don't know what version to return
					ret = terminator.acquireVersion();
					break outer;
				}

				// bb is not what we wanted but we need to unlink it and start over
				if (bb.isTerminated()) {
					if (STATISTICS)
						statsArray[6]++;

					if (bbb == null) {
						if (STATISTICS)
							statsArray[7]++;

						// moving bb to the left, but we could jump by more than one node
						bb = findPredecessor(bb.key, comparator);
						continue outer;
					}
					unlinkNode(bbb, bb);
					// moving right
					bb = bbb.acquireNext();
					continue;
				}

				nb = bb.acquireNext();
				// check if we found the node with the terminator we've started with; if so we
				// break out of the loop
				if (nb == b) {
					// strange case
					if (bb.getType() == Node.TEMP_SPLIT) {
						if (bbb == null) {
							if (STATISTICS)
								statsArray[8]++;
							
							helpTempSplitNode((TempSplitNode<K, V>) bb, null);
							bb = findPredecessor(bb.key, comparator);
							continue outer;
						} else {
							if (STATISTICS)
								statsArray[9]++;
							
							helpTempSplitNode((TempSplitNode<K, V>) bb, bbb);
							Revision<K, V> bbbHead = bbb.acquireRevisionHead();
							helpPut(bbbHead);
							bb = null;
							
							continue outer;
						}
					}
					break;
				}

				// move right
				bbb = bb;
				bb = nb;
			}

			Revision<K, V> head = bb.acquireRevisionHead();

			if (head.effectiveVersion() < 0) {
				if (STATISTICS)
					statsArray[10]++;

				helpPut(head);

				// optimization
				Revision<K, V> newHead = bb.acquireRevisionHead();
				if (newHead != head)
					continue;
			}

			if (b.isTerminated()) {
				if (STATISTICS)
					statsArray[11]++;

				cleanTerminatedNode(b.key);
				tryReduceLevel();

				ret = terminator.acquireVersion();
				break outer;
			}

			if (bb.acquireNext() != b) {
				if (STATISTICS)
					statsArray[12]++;

				bb = null;
				continue;
			}

			// seems redundant (see helpPut above, mergeTermiantor will have negative version)
			if (head.getValue() == null) {
				if (STATISTICS)
					statsArray[13]++;

				bb = null;
				continue;
			}

			MultiVal<K, V> mval = new SingleMultiVal<K, V>(comparator, head.getValue(), terminator.next.getValue(),
					terminator.indexOfKeyInNextMultiVal);

			MergeRevision<K, V> mergeRevision = new MergeRevision<>(mval, terminator.version, head, terminator.next,
					nb, terminator);

			mergeRevision.setAutoscaleParam(terminator.getAutoscaleParam());

			if (bb.tryPutRevisionSingle(mergeRevision)) {
				if (STATISTICS)
					statsArray[14]++;

				ret = helpMerge(mergeRevision);
				break outer;
			}
			bb = null;
		}

		if (STATISTICS)
			stats.get().updateHelpMergeTerminator(statsArray);
		return ret;
	}

	private long helpMergeTerminatorBatch(MergeTerminatorRevision<K, V> terminator) {
		long[] statsArray = null;
		if (STATISTICS)
			statsArray = new long[17];
		
		long ret;
		
		Node<K, V> b = terminator.node;
		Node<K, V> bb = null;
		
		outer: while (true) {
			if (STATISTICS)
				statsArray[0]++;

			if (b.isTerminated()) {
				if (STATISTICS)
					statsArray[1]++;

				cleanTerminatedNode(b.key);
				tryReduceLevel();
				
				ret = terminator.descriptor.acquireVersion();
				break outer;
			}

			Node<K, V> bbb = null;
			Node<K, V> nb = null; // new b
			if (bb == null) {
				if (STATISTICS)
					statsArray[2]++;

				bb = findPredecessor(b.key, comparator);
			}

			while (true) {
				if (STATISTICS)
					statsArray[3]++;

				if (bb == null || (bb.key != null && cpr(comparator, bb.key, b.key) > 0)) {
					if (STATISTICS)
						statsArray[4]++;

					if (!b.isTerminated()) {
						if (STATISTICS)
							statsArray[5]++;

						// TODO why is this happening?
						bb = null;
						continue outer;
					}
					// hack, because we don't know what version to return
					ret = terminator.effectiveVersion();
					break outer;
				}

				if (bb.isTerminated()) {
					if (STATISTICS)
						statsArray[6]++;

					if (bbb == null) {
						if (STATISTICS)
							statsArray[7]++;

						bb = findPredecessor(bb.key, comparator);
						continue outer;
					}
					unlinkNode(bbb, bb);
					bb = bbb.acquireNext();
					continue;
				}

				nb = bb.acquireNext();
				if (nb == b) {
					// strange case
					if (bb.getType() == Node.TEMP_SPLIT) {
						if (bbb == null) {
							if (STATISTICS)
								statsArray[8]++;

							helpTempSplitNode((TempSplitNode<K, V>) bb, null);
							bb = findPredecessor(bb.key, comparator);
							continue outer;
						} else {
							if (STATISTICS)
								statsArray[9]++;

							helpTempSplitNode((TempSplitNode<K, V>) bb, bbb);
							Revision<K, V> bbbHead = bbb.acquireRevisionHead();
							helpPut(bbbHead);
							bb = null;
							continue outer;
						}
					}
					break;
				}

				bbb = bb;
				bb = nb;
			}

			Revision<K, V> head = bb.acquireRevisionHead();

			if (b.isTerminated()) {
				if (STATISTICS)
					statsArray[10]++;

				cleanTerminatedNode(b.key);
				tryReduceLevel();

				ret = terminator.descriptor.acquireVersion();
				break outer;
			}

			if (head.descriptor == terminator.descriptor) {
				if (STATISTICS)
					statsArray[11]++;

				ret = helpMerge((MergeRevision<K, V>) head);
				break outer;
			}

			if (head.effectiveVersion() < 0 && head != terminator) {
				if (STATISTICS)
					statsArray[12]++;

				helpPut(head);

				// optimization
				Revision<K, V> newHead = bb.acquireRevisionHead();
				if (newHead != head)
					continue;
			}

			long version = terminator.descriptor.acquireVersion();
			if (b.isTerminated()) {
				if (STATISTICS)
					statsArray[13]++;

				cleanTerminatedNode(b.key);
				tryReduceLevel();

				ret = terminator.effectiveVersion();
				break outer;
			}

			if (bb.acquireNext() != b) {
				if (STATISTICS)
					statsArray[14]++;

				bb = null;
				continue;
			}

			if (head.getValue() == null) {
				if (STATISTICS)
					statsArray[15]++;

				bb = null;
				continue;
			}

			MultiValIndices<K> terminatorIndices = terminator.nextIndices;

			Batch<K, V> batch = terminator.descriptor.batch;
			if (batch == null) {
				ret = terminator.descriptor.acquireVersion();
				break outer;
			}

			int indexOfRightmostRelevantBatchKey = terminator.indexOfLeftmostRelevantBatchKey - 1;

			MultiValIndices<K> headIndices = head.getValue().indexOfKeysInMultiVal(batch,
					indexOfRightmostRelevantBatchKey, bb.key);

			MultiVal<K, V> mval = new SingleMultiVal<K, V>(comparator, batch, head.getValue(),
					terminator.next.getValue(), headIndices, terminatorIndices);

			assert version < 0;

			int indexOfFirstRelevantKeyInBatch = 0;
			if (headIndices.indices.length == 0)
				indexOfFirstRelevantKeyInBatch = terminator.indexOfLeftmostRelevantBatchKey;
			else
				indexOfFirstRelevantKeyInBatch = headIndices.indexOfFirstRelevantKeyInBatch;

			MergeRevision<K, V> mergeRevision = new MergeRevision<>(mval, version, terminator.descriptor, bb.key,
					indexOfFirstRelevantKeyInBatch, indexOfRightmostRelevantBatchKey, null, 
					head, headIndices, terminator.next, nb, terminator);

			mergeRevision.setAutoscaleParam(terminator.getAutoscaleParam());

			if (bb.tryPutRevisionSingle(mergeRevision)) {
				if (STATISTICS)
					statsArray[16]++;

				ret = helpMerge(mergeRevision);
				break outer;
			}
			bb = null;
		}

		if (STATISTICS)
			stats.get().updateHelpMergeTerminatorBatch(statsArray);
		return ret;
	}

	MergeRevision<K, V> findMergeRevisionSingle(MergeTerminatorRevision<K, V> terminator, long cutOffVersion) {
		Node<K, V> tn = terminator.node;
		Node<K, V> b;

		MergeRevision<K, V> ret = null;

		long[] statsArray = null;
		if (STATISTICS)
			statsArray = new long[6];

		VarHandle.acquireFence();

		Comparator<? super K> cmp = comparator;
		outer: for (;;) {
			if (STATISTICS)
				statsArray[0]++;

			VarHandle.acquireFence();
			b = findPredecessor(tn.key, cmp);

			assert b != null; // because we have at least one MultiVal
			Node<K, V> n = null;
			for (;;) { // find insertion point
				if (STATISTICS)
					statsArray[1]++;
				int c = 0;
				n = b.next;

				K k = n != null ? n.key : null;
				if (n == null) {
					c = -1;
				} else if (n.isTerminated()) {
					unlinkNode(b, n);
					c = 1;
				} else if ((c = cpr(cmp, tn.key, k)) > 0)
					b = n;

				if (c <= 0) {
					if (b.getType() == Node.TEMP_SPLIT) {
						if (STATISTICS)
							statsArray[2]++;
						helpTempSplitNode((TempSplitNode<K, V>) b, null);
						continue outer;
					}

					Revision<K, V> head = b.acquireRevisionHead();

					if (b.isTerminated()) {
						if (STATISTICS)
							statsArray[3]++;
						continue outer; // retry
					}

					if (head.getType() == Revision.MERGE
							&& ((MergeRevision<K, V>) head).mergeTerminator == terminator) {
						if (STATISTICS)
							statsArray[4]++;

						helpPut(head);
						ret = (MergeRevision<K, V>) head;
						break outer;
					}

					Revision<K, V> currentRevision = head;

					while (currentRevision != null) {
						if (STATISTICS)
							statsArray[5]++;
						long version = currentRevision.effectiveVersion();

						if (currentRevision.getType() == Revision.MERGE) {
							MergeRevision<K, V> mergeRevision = (MergeRevision<K, V>) currentRevision;
							if (version > 0) {
								if (mergeRevision.mergeTerminator == terminator) {
									ret = mergeRevision;
									break outer;
								}

								if (version < cutOffVersion) {
									ret = null;
									break outer;
								}
							}

							if (cpr(cmp, tn.key, mergeRevision.keyOfRightNode) > 0) {
								currentRevision = mergeRevision.acquireRightNext();
								continue;
							}
						}
						if (version > 0 && version < cutOffVersion) {
							ret = null;
							break outer;
						}
						currentRevision = currentRevision.acquireNext();
					}

					String errorMsg = String.format(
							"findMergeRevision(): Should not happen, looking for key < %s, cutOffVersion: %d\n- term: %s\n- b: %s\n- n: %s\n",
							tn.key, cutOffVersion, terminator, b, n);
					
					throw new JiffyInternalException(errorMsg);
				}
			}
		}

		if (STATISTICS)
			stats.get().updateFindMergeTerminator(statsArray, cutOffVersion == 0);

		return ret;
	}

	MergeRevision<K, V> findMergeRevisionBatch(MergeTerminatorRevision<K, V> terminator, boolean primaryRun) {
		Node<K, V> tn = terminator.node;
		BatchDescriptor<K, V> descriptor = terminator.descriptor;
		assert descriptor != null;
		Node<K, V> b;

		MergeRevision<K, V> ret = null;

		long[] statsArray = null;
		if (STATISTICS)
			statsArray = new long[6];

		VarHandle.acquireFence();

		Comparator<? super K> cmp = comparator;
		outer: for (;;) {
			if (terminator.descriptor.acquireVersion() > 0 && !primaryRun)
				break outer;

			if (STATISTICS)
				statsArray[0]++;

			VarHandle.acquireFence();
			b = findPredecessor(tn.key, cmp);

			assert b != null; // because we have at least one MultiVal
			Node<K, V> n = null;
			for (;;) { // find insertion point
				if (STATISTICS)
					statsArray[1]++;
				int c = 0;
				n = b.next;

				K k = n != null ? n.key : null;
				if (n == null) {
					c = -1;
				} else if (n.isTerminated()) {
					unlinkNode(b, n);
					c = 1;
				} else if ((c = cpr(cmp, tn.key, k)) > 0)
					b = n;

				if (c <= 0) {
					if (b.getType() == Node.TEMP_SPLIT) {
						if (STATISTICS)
							statsArray[2]++;
						helpTempSplitNode((TempSplitNode<K, V>) b, null);
						continue outer;
					}

					Revision<K, V> head = b.acquireRevisionHead();

					if (b.isTerminated()) {
						if (STATISTICS)
							statsArray[3]++;
						continue outer; // retry; break to outer?
					}

					if (STATISTICS)
						statsArray[5]++;

					if (primaryRun) {
						Revision<K, V> currentRevision = head;
						while (currentRevision != null) {
							if (STATISTICS)
								statsArray[5]++;

							if (currentRevision.getType() == Revision.MERGE) {
								MergeRevision<K, V> mergeRevision = (MergeRevision<K, V>) currentRevision;
								if (mergeRevision.mergeTerminator == terminator) {
									ret = mergeRevision;
									break outer;
								}

								if (cpr(cmp, tn.key, mergeRevision.keyOfRightNode) > 0) {
									currentRevision = mergeRevision.acquireRightNext();
									continue;
								}
							}
							currentRevision = currentRevision.acquireNext();
						}

						String errorMsg = String.format(
								"findMergeRevisionWithDescriptor(): Should not happen, looking for key < %s, cutOffVersion: %d\n- term: %s\n- b: %s\n- n: %s\n",
								tn.key, 0, terminator, b, n);

						throw new JiffyInternalException(errorMsg);
					} else {
						if (head.getType() == Revision.MERGE) {
							MergeRevision<K, V> mergeRevision = (MergeRevision<K, V>) head;
							if (mergeRevision.mergeTerminator == terminator)
								ret = mergeRevision;
						}
						// merge and the whole batch must be finished, because the merge revision
						// is 'covered' by some other revision
						break outer;
					}
				}
			}
		}

//		if (STATISTICS)
//			stats.get().updateFindMergeTerminator(statsArray, cutOffVersion == 0);

		return ret;
	}

	/**
	 * Main insertion method for batches. Adds element if not present, or replaces
	 * value if present and onlyIfAbsent is false.
	 *
	 * @param key          the key
	 * @param value        the value that must be associated with key
	 * @param onlyIfAbsent if should not insert if already present
	 * @return the newly inserted revision
	 */
	private Revision<K, V> doPutBatch(BatchDescriptor<K, V> descriptor, int indexOfFirstKeyFromRight,
			boolean primaryRun) {
		long[] statsArray = null;
		if (STATISTICS)
			statsArray = new long[12];

		Revision<K, V> ret = null;

		Batch<K, V> batch = descriptor.batch;
		long version = descriptor.acquireVersion();
		long optimisticVersion = version < 0 ? -version : 0;

		K firstKeyFromRight = batch != null ? batch.getKeyByIndex(indexOfFirstKeyFromRight) : null;

		K key = firstKeyFromRight;

		Comparator<? super K> cmp = comparator;
		Revision<K, V> revision = new Revision<K, V>(null, -optimisticVersion, descriptor, null, -1,
				indexOfFirstKeyFromRight, firstKeyFromRight, null, null);

		outer: for (;;) {
			if (!primaryRun && descriptor.acquireVersion() > 0)
				break;

			if (STATISTICS)
				statsArray[0]++;

			Index<K, V> h;
			Node<K, V> b;
			VarHandle.acquireFence();
			int levels = 0; // number of levels descended
			h = head;
			for (Index<K, V> q = h, r, d;;) { // count while descending
				while ((r = q.right) != null) {
					Node<K, V> p = r.node;
					K k = p != null ? p.key : null;
					if (p == null || p.isTerminated())
						RIGHT.compareAndSet(q, r, r.right);
					else if (cpr(cmp, key, k) > 0)
						q = r;
					else
						break;
				}
				if ((d = q.down) != null) {
					++levels;
					q = d;
				} else {
					b = q.node;
					break;
				}
			}

			assert b != null; // because we have at least one MultiVal
			Node<K, V> n;
			insertionPoint: for (;;) { // find insertion point
				if (STATISTICS)
					statsArray[1]++;

				int c;
				n = b.next;
				K k = n != null ? n.key : null;
				if (n == null) {
					if (b.key == null) // if empty, type check key now
						cpr(cmp, key, key);
					c = -1;
				} else if (n.isTerminated()) {
					unlinkNode(b, n);
					c = 1;
				} else if ((c = cpr(cmp, key, k)) >= 0)
					b = n;

				if (c < 0) {
					if (b.getType() == Node.TEMP_SPLIT) {
						if (STATISTICS)
							statsArray[2]++;

						helpTempSplitNode((TempSplitNode<K, V>) b, null);
						continue outer;
					}

					Revision<K, V> head = b.acquireRevisionHead();

					long currentVersion = descriptor.acquireVersion();

					if (b.isTerminated()) {
						if (STATISTICS)
							statsArray[3]++;

						continue outer; // retry
					}

					if (b.acquireNext() != n) {
						if (STATISTICS)
							statsArray[4]++;

						continue insertionPoint;
					}

					if (head.descriptor == descriptor) {
						if (STATISTICS)
							statsArray[5]++;

						Revision<K, V> helpedRevision = helpBatchRevision(head, primaryRun);

						if (primaryRun || (helpedRevision != null && helpedRevision.effectiveVersion() < 0))
							ret = helpedRevision;
						break outer;
					}

					if (head.effectiveVersion() < 0 && currentVersion < 0) {
						if (STATISTICS)
							statsArray[6]++;

						helpPut(head);
						continue insertionPoint;
					}

					if (b.acquireNext() != n) {
						if (STATISTICS)
							statsArray[7]++;

						continue insertionPoint;
					}

					if (currentVersion > 0) {
						if (STATISTICS)
							statsArray[8]++;

						if (primaryRun) {
							Revision<K, V> retrievedRevision = retrieveBatchRevision(b, key, descriptor, head);
							if (retrievedRevision == null)
								continue outer;
							ret = retrievedRevision;
						} else
							ret = null;
						break outer;
					}

					MultiVal<K, V> headMval = head.getValue();
					MultiValIndices<K> indicesAndEndSize = headMval.indexOfKeysInMultiVal(batch,
							indexOfFirstKeyFromRight, b.key);

					long delta = (long) (1.0 * (optimisticVersion - autoscalingLastTimeSet.get())
							* indicesAndEndSize.indices.length / batch.size());
					double[] newAutoscaleParam = newAutoscaleParamForUpdates(head.getAutoscaleParam(), delta);

					int updateType = whatUpdate(indicesAndEndSize.endSize, newAutoscaleParam, head);
					if (updateType == -1 && b.key == null)
						updateType = 0;

					if (updateType == 0) {
						if (STATISTICS)
							statsArray[9]++;

						MultiVal<K, V> mval = headMval.add(batch, indicesAndEndSize);
						revision.value = null;
						revision.setValue(mval, indicesAndEndSize);
						revision.next = head;
						revision.setAutoscaleParam(newAutoscaleParam);

						revision.indexOfLeftmostRelevantBatchKey = indicesAndEndSize.indexOfFirstRelevantKeyInBatch;


						if (b.tryPutRevisionSingle(revision)) {
							ret = revision;
							break outer;
						}
						continue insertionPoint;
					} else if (updateType == 1) {
						if (STATISTICS)
							statsArray[10]++;

						Pair<MultiVal<K, V>, MultiVal<K, V>> mvalPair = head.getValue().addAndSplit(batch,
								indicesAndEndSize);
						SplitRevision<K, V> leftRevision = new SplitRevision<>(mvalPair.first, revision.version,
								descriptor, b.key, indicesAndEndSize.indexOfFirstRelevantKeyInBatch,
								indexOfFirstKeyFromRight, null, // revision.rightmostRelevantBatchKey,
								head, indicesAndEndSize, true);
						SplitRevision<K, V> rightRevision = new SplitRevision<>(mvalPair.second, revision.version,
								descriptor, mvalPair.second.firstKey(),
								indicesAndEndSize.indexOfFirstRelevantKeyInBatch, indexOfFirstKeyFromRight, null, // revision.rightmostRelevantBatchKey,
								head, null, false);
						leftRevision.sibling = rightRevision;
						rightRevision.sibling = leftRevision;

						leftRevision.setAutoscaleParam(newAutoscaleParam);
						rightRevision.setAutoscaleParam(newAutoscaleParam);

						leftRevision.node = b;
						leftRevision.levels = levels;


						if (!b.tryPutRevisionSingle(leftRevision))
							continue insertionPoint; // retry 

						if (SPLIT_MERGE_STATISTICS)
							splitCounter.set(splitCounter.get() + 1);

						revision = leftRevision;

						helpSplit(leftRevision);

						ret = revision;
						break outer;
					} else {
						if (STATISTICS)
							statsArray[11]++;

						MergeTerminatorRevision<K, V> mergeTerminator = new MergeTerminatorRevision<>(revision.version,
								descriptor, b.key, indicesAndEndSize.indexOfFirstRelevantKeyInBatch,
								indexOfFirstKeyFromRight, null, 
								b, head, indicesAndEndSize);
						mergeTerminator.setAutoscaleParam(newAutoscaleParam);
						if (b.tryPutRevisionSingle(mergeTerminator)) {
							if (SPLIT_MERGE_STATISTICS)
								mergeCounter.set(mergeCounter.get() + 1);

							helpMergeTerminator(mergeTerminator);
							ret = mergeTerminator; // not returning the true merge revision!
							break outer;
						}
						// carries on, i.e., continue insertionPoint
					}
				}
			}
		}

		if (STATISTICS) {
			if (primaryRun)
				stats.get().updateDoPutBatchPrimaryRun(statsArray);
			else
				stats.get().updateDoPutBatchHelperRun(statsArray);
		}

		return ret;
	}

	/**
	 * Add indices after an insertion. Descends iteratively to the highest level of
	 * insertion, then recursively, to chain index nodes to lower ones. Returns null
	 * on (staleness) failure, disabling higher-level insertions. Recursion depths
	 * are exponentially less probable.
	 *
	 * @param q     starting index for current level
	 * @param skips levels to skip before inserting
	 * @param x     index for this insertion
	 * @param cmp   comparator
	 */
	static <K, V> boolean addIndices(Index<K, V> q, int skips, Index<K, V> x, Comparator<? super K> cmp) {
		Node<K, V> z;
		K key;
		if (x != null && (z = x.node) != null && (key = z.key) != null && q != null) { // hoist checks
			boolean retrying = false;
			for (;;) { // find splice point
				Index<K, V> r, d;
				int c;
				if ((r = q.right) != null) {
					Node<K, V> p;
					K k;
					// if ((p = r.node) == null || (k = p.key) == null || p.val == null) {
					if ((p = r.node) == null || (k = p.key) == null || p.isTerminated()) {
						RIGHT.compareAndSet(q, r, r.right);
						c = 0;
					} else if ((c = cpr(cmp, key, k)) > 0)
						q = r;
					else if (c == 0)
						break; // stale
				} else
					c = -1;

				if (c < 0) {
					if ((d = q.down) != null && skips > 0) {
						--skips;
						q = d;
					} else if (d != null && !retrying && !addIndices(d, 0, x.down, cmp))
						break;
					else {
						x.right = r;
						if (RIGHT.compareAndSet(q, r, x))
							return true;
						else
							retrying = true; // re-find splice point
					}
				}
			}
		}
		return false;
	}

	/* ---------------- Deletion -------------- */

	final void cleanTerminatedNode(K key) {
		if (key == null)
			throw new NullPointerException();
		Comparator<? super K> cmp = comparator;
		Node<K, V> b;
		outer: while ((b = findPredecessor(key, cmp)) != null) {
			for (;;) {
				Node<K, V> n;
				n = b.next;
				K k = n != null ? n.key : null;
				if (n == null)
					break outer;
				else {
					if (n.isTerminated())
						unlinkNode(b, n);

					if (cpr(cmp, key, k) > 0)
						b = n;
					else
						break outer;
				}
			}
		}
	}

	/**
	 * Possibly reduce head level if it has no nodes. This method can (rarely) make
	 * mistakes, in which case levels can disapp)ear even though they are about to
	 * contain index nodes. This impacts performance, not correctness. To minimize
	 * mistakes as well as to reduce hysteresis, the level is reduced by one only if
	 * the topmost three levels look empty. Also, if the removed level looks
	 * non-empty after CAS, we try to change it back quick before anyone notices our
	 * mistake! (This trick works pretty well because this method will practically
	 * never make mistakes unless current thread stalls immediately before first
	 * CAS, in which case it is very unlikely to stall again immediately afterwards,
	 * so will recover.)
	 *
	 * We put up with all this rather than just let levels grow because otherwise,
	 * even a small map that has undergone a large number of insertions and removals
	 * will have a lot of levels, slowing down access more than would an occasional
	 * unwanted reduction.
	 */
	private void tryReduceLevel() {
		Index<K, V> h, d, e;
		if ((h = head) != null && h.right == null && (d = h.down) != null && d.right == null && (e = d.down) != null
				&& e.right == null && HEAD.compareAndSet(this, h, d) && h.right != null) // recheck
			HEAD.compareAndSet(this, d, h); // try to backout
	}

	/* ---------------- Finding and removing first element -------------- */

	/**
	 * Gets first valid node, unlinking deleted nodes if encountered.
	 * 
	 * @return first node or null if empty
	 */
	final Quad<Node<K, V>, Revision<K, V>, Integer, Node<K, V>> findFirst() {
		return findFirst(NEWEST_VERSION);
	}

	final Quad<Node<K, V>, Revision<K, V>, Integer, Node<K, V>> findFirst(long version) {
		Node<K, V> n = baseHead();
		while (true) {
			if (n == null)
				return null;

			Node<K, V> nNext = n.acquireNext();

			Revision<K, V> head = null;
			if (n.getType() == Node.TEMP_SPLIT) {
				head = ((TempSplitNode<K, V>) n).leftRevision;
				Node<K, V> leftNode = ((SplitRevision<K, V>) head).node;
				if (head.effectiveVersion() > 0) {
					// we may have observed a faulty TempSplitNode introduced through ABA
					if (leftNode.acquireNext() == n) {
						n.terminate();
						unlinkNode(leftNode, n);
					}
					n = baseHead();
					continue;
				}
				n = leftNode;
			} else if ((head = n.acquireRevisionHead()).getType() == Revision.MERGE_TERMINATOR) {
				helpMergeTerminator((MergeTerminatorRevision<K, V>) head);
				cleanTerminatedNode(n.key);
				n = baseHead();
				continue;
			}

			Revision<K, V> revision = retrieveRevision(n, null, version, head);
			if (revision != null && revision.getValue().firstValue() != null)
				return new Quad<>(n, revision, 0, nNext);

			n = nNext;
		}
	}

	/**
	 * Entry snapshot version of findFirst
	 */
	final AbstractMap.SimpleImmutableEntry<K, V> findFirstEntry() {
		return findFirstEntry(NEWEST_VERSION);
	}

	final AbstractMap.SimpleImmutableEntry<K, V> findFirstEntry(long version) {
		throw new UnsupportedOperationException();
//		TODO commented out
//		Node<K, V> b, n;
//		V v;
//		if ((b = baseHead()) != null) {
//			while ((n = b.next) != null) {
//				// if ((v = n.val) == null)
//				if (n.isTerminated())
//					unlinkNode(b, n);
//				else if ((v = getProperValue(n, version)) != null)
//					return new AbstractMap.SimpleImmutableEntry<K, V>(n.key, v);
//			}
//		}
//		return null;
	}

	/**
	 * Removes first entry; returns its snapshot.
	 * 
	 * @return null if empty, else snapshot of first entry
	 */
	private AbstractMap.SimpleImmutableEntry<K, V> doRemoveFirstEntry() {
		return doRemoveFirstEntry(NEWEST_VERSION);
	}

	private AbstractMap.SimpleImmutableEntry<K, V> doRemoveFirstEntry(long version) {
		throw new UnsupportedOperationException();
//        Node<K,V> b, n; V v;
//        if ((b = baseHead()) != null) {
//            while ((n = b.next) != null) {
//                if ((v = n.val) == null || VAL.compareAndSet(n, v, null)) {
//                    K k = n.key;
//                    unlinkNode(b, n);
//                    if (v != null) {
//                        tryReduceLevel();
//                        findPredecessor(k, comparator); // clean index
//                        addCount(-1L);
//                        return new AbstractMap.SimpleImmutableEntry<K,V>(k, v);
//                    }
//                }
//            }
//        }
//        return null;
	}

	/* ---------------- Finding and removing last element -------------- */

	/**
	 * Specialized version of find to get last valid node.
	 * 
	 * @return last node or null if empty
	 */
	final Quad<Node<K, V>, Revision<K, V>, Integer, Node<K, V>> findLast() {
		return findLast(NEWEST_VERSION);
	}

	final Quad<Node<K, V>, Revision<K, V>, Integer, Node<K, V>> findLast(long version) {
		Node<K, V> b = baseHead();
		Node<K, V> bNext = null;
		Revision<K, V> bHead = null;

		outer: for (;;) {
			Index<K, V> q;
			VarHandle.acquireFence();
			if ((q = head) == null)
				break;
			for (Index<K, V> r, d;;) {
				while ((r = q.right) != null) {
					Node<K, V> p;
					if ((p = r.node) == null || p.isTerminated())
						RIGHT.compareAndSet(q, r, r.right);
					else
						q = r;
				}
				if ((d = q.down) != null)
					q = d;
				else {
					b = q.node;
					break;
				}
			}
			if (b != null) {
				for (;;) {
					Node<K, V> n;
					if ((n = b.next) == null) {
						if (b.key == null) // empty
							break;
						else
							break;
					} else if (n.isTerminated())
						unlinkNode(b, n);
					else
						b = n;
				}
			}

			bNext = b.acquireNext();

			if (b.getType() == Node.TEMP_SPLIT) {
				bHead = ((TempSplitNode<K, V>) b).leftRevision;
				Node<K, V> leftNode = ((SplitRevision<K, V>) bHead).node;
				if (bHead.effectiveVersion() > 0) {
					// we may have observed a faulty TempSplitNode introduced through ABA
					if (leftNode.acquireNext() == b) {
						b.terminate();
						unlinkNode(leftNode, b);
					}
					continue outer;
				}
				b = leftNode;
			} else if ((bHead = b.acquireRevisionHead()).getType() == Revision.MERGE_TERMINATOR) {
				helpMergeTerminator((MergeTerminatorRevision<K, V>) bHead);
				cleanTerminatedNode(b.key);
				continue outer;
			}

			break;
		}

		var triple = new Triple<>(b, retrieveRevision(b, null, version, bHead), 0);
		if (triple != null && triple.second != null) {
			MultiVal<K, V> mval = triple.second.getValue();
			if (mval.size() > 0) {
				// TODO we create an object because triple has final fields
				return new Quad<>(triple.first, triple.second, mval.size() - 1, bNext);
			}
		}

		if (b.key == null)
			return null;

		return findNear(b.key, LT, comparator, version);
	}

	/**
	 * Entry version of findLast
	 * 
	 * @return Entry for last node or null if empty
	 */
	final AbstractMap.SimpleImmutableEntry<K, V> findLastEntry() {
		return findLastEntry(NEWEST_VERSION);
	}

	final AbstractMap.SimpleImmutableEntry<K, V> findLastEntry(long version) {
		throw new UnsupportedOperationException();
//		for (;;) {
//			Node<K, V> n;
//			V v;
//			if ((n = findLast()) == null)
//				return null;
//			// if ((v = n.val) != null)
//			// return new AbstractMap.SimpleImmutableEntry<K,V>(n.key, v);
//			if (n.isTerminated())
//				return null;
//			Revision<K, V> revision = n.getNewestRevision();
//			if (revision != null)
//				return new AbstractMap.SimpleImmutableEntry<K, V>(n.key, revision.value);
//			return null;
//		}
	}

	/**
	 * Removes last entry; returns its snapshot. Specialized variant of doRemove.
	 * 
	 * @return null if empty, else snapshot of last entry
	 */
	private Map.Entry<K, V> doRemoveLastEntry() {
		return doRemoveLastEntry(NEWEST_VERSION);
	}

	private Map.Entry<K, V> doRemoveLastEntry(long version) {
		throw new UnsupportedOperationException();
//        outer: for (;;) {
//            Index<K,V> q; Node<K,V> b;
//            VarHandle.acquireFence();
//            if ((q = head) == null)
//                break;
//            for (;;) {
//                Index<K,V> d, r; Node<K,V> p;
//                while ((r = q.right) != null) {
//                    if ((p = r.node) == null || p.val == null)
//                        RIGHT.compareAndSet(q, r, r.right);
//                    else if (p.next != null)
//                        q = r;  // continue only if a successor
//                    else
//                        break;
//                }
//                if ((d = q.down) != null)
//                    q = d;
//                else {
//                    b = q.node;
//                    break;
//                }
//            }
//            if (b != null) {
//                for (;;) {
//                    Node<K,V> n; K k; V v;
//                  )  if ((n = b.next) == null) {
//                        if (b.key == null) // empty
//                            break outer;
//                        else
//                            break; // retry
//                    }
//                    else if ((k = n.key) == null)
//                        break;
//                    else if ((v = n.val) == null)
//                        unlinkNode(b, n);
//                    else if (n.next != null)
//                        b = n;
//                    else if (VAL.compareAndSet(n, v, null)) {
//                        unlinkNode(b, n);
//                        tryReduceLevel();
//                        findPredecessor(k, )comparator); // clean index
//                        addCount(-1L);
//                        return new AbstractMap.SimpleImmutableEntry<K,V>(k, v);
//                    }
//                }
//            }
//        }
//        return null;
	}

	/************ Relational operations **************/

	// Control values OR'ed as arguments to findNear

	static final int EQ = 1; // Used only in conjunction with GT or LT
	static final int LT = 2;
	static final int GT = 0; // Actually checked as !LT

	/**
	 * Utility for ceiling, floor, lower, higher methods.
	 * 
	 * @param key the key
	 * @param rel the relation -- OR'ed combination of EQ, LT, GT
	 * @return nearest node fitting relation, or null if no such
	 */
	final Quad<Node<K, V>, Revision<K, V>, Integer, Node<K, V>> findNear(K key, int rel, Comparator<? super K> cmp,
			long version) {
		if (key == null)
			throw new NullPointerException();

		long[] statsArray = null;
		if (STATISTICS)
			statsArray = new long[5];

		Quad<Node<K, V>, Revision<K, V>, Integer, Node<K, V>> ret = null;

		Node<K, V> b, n, bb, nNext;
		K searchKey = key;
		K doNotExceedKey = null;
		outer: while ((b = findPredecessor(searchKey, cmp)) != null) {
			if (STATISTICS)
				statsArray[0]++;
			bb = b;

			for (;;) {
				if (STATISTICS)
					statsArray[1]++;
				n = b.acquireNext();
				K k = n != null ? n.key : null;

				if (n == null) {
					n = b;
					b = b.key == null ? null : bb;
					break; // empty
				} else if (n.isTerminated()) {
					unlinkNode(b, n); // n is deleted
					continue;
				} else if (doNotExceedKey != null && cpr(cmp, doNotExceedKey, k) <= 0) {
					// n is too far
					n = b;
					b = b.key == null ? null : bb;
					doNotExceedKey = null;
					break;
				} else if (doNotExceedKey == null && cpr(cmp, searchKey, k) < 0) {
					// n is too far
					n = b;
					b = b.key == null ? null : bb;
					break;
				}

				bb = b;
				b = n;
			}

			while (true) {
				if (STATISTICS)
					statsArray[2]++;
				if (n == null) {
					break outer;
				}

				nNext = n.acquireNext();
				Revision<K, V> head = n.acquireRevisionHead();

				if (n.getType() == Node.TEMP_SPLIT) {
					if (STATISTICS)
						statsArray[3]++;
					helpTempSplitNode((TempSplitNode<K, V>) n, null);
					
					continue outer;
				}

				if (head.getType() == Revision.MERGE_TERMINATOR) {
					helpMergeTerminator((MergeTerminatorRevision<K, V>) head);
					cleanTerminatedNode(n.key);
					if (STATISTICS)
						statsArray[4]++;
					continue outer;
				}

				Revision<K, V> revision = retrieveRevision(n, null, version, head);

				if (revision != null) {
					MultiVal<K, V> mval = revision.getValue();
					int index = mval.indexOfKeyInMultiVal(key);
					int mvalSize = mval.size();

					if (index >= 0) {
						if ((rel & EQ) != 0) {
							ret = new Quad<>(n, revision, index, nNext);
							break outer;
						}

						if ((rel & LT) != 0) {
							if (index >= 1) {
								ret = new Quad<>(n, revision, index - 1, nNext);
								break outer;
							} else {
								if (n.key == null) {
									// ret = null;
									break outer;
								}
							}
						} else {
							if (index < mvalSize - 1) {
								ret = new Quad<>(n, revision, index + 1, nNext);
								break outer;
							}
						}
					} else {
						int insPoint = -index - 1;

						if ((rel & LT) != 0) {
							if (insPoint >= 1) {
								ret = new Quad<>(n, revision, insPoint - 1, nNext);
								break outer;
							} else {
								if (n.key == null) {
									// ret = null;
									break outer;
								}
							}
						} else {
							if (insPoint < mvalSize) {
								ret = new Quad<>(n, revision, insPoint, nNext);
								break outer;
							}
						}
					}
				}

				if ((rel & LT) != 0) {
					if (b == null) {
						break outer;
					}

					if (b.key == null) {
						rel &= ~EQ;
						n = b;
						b = null;
					} else {
						searchKey = b.key; // we need a predecessor!
						doNotExceedKey = n.key;
						continue outer;
					}
				} else {
					n = nNext;
				}
			}
		}

		if (STATISTICS)
			stats.get().updateFindNear(statsArray);

		return ret;
	}

	/**
	 * Variant of findNear returning SimpleImmutableEntry
	 * 
	 * @param key the key
	 * @param rel the relation -- OR'ed combination of EQ, LT, GT
	 * @return Entry fitting relation, or null if no such
	 */
	final Map.Entry<K, V> findNearEntry(K key, int rel, Comparator<? super K> cmp) {
		return findNearEntry(key, rel, cmp, NEWEST_VERSION);
	}

	final Map.Entry<K, V> findNearEntry(K key, int rel, Comparator<? super K> cmp, long version) {
		for (;;) {
			Quad<Node<K, V>, Revision<K, V>, Integer, Node<K, V>> t;
			if ((t = findNear(key, rel, cmp, version)) == null)
				return null;
			if (t.first.isTerminated())
				continue;
			return t.second.getValue().getByIndex(t.third);
		}
	}

	/* ---------------- Constructors -------------- */

	final long startTime;

	/**
	 * Constructs a new, empty map, sorted according to the {@linkplain Comparable
	 * natural ordering} of the keys.
	 */
	public Jiffy() {
		startTime = System.nanoTime();
		this.comparator = null;
		initializeHead();
	}

	public Jiffy(int maxMultivalSize, int minMultivalSize) {
		startTime = System.nanoTime();
		this.MAX_MULTIVAL_SIZE = maxMultivalSize;
		this.MIN_MULTIVAL_SIZE = minMultivalSize;
		this.comparator = null;
		initializeHead();
		System.err.println(String.format("Jiffy(%d, %d)", MAX_MULTIVAL_SIZE, MIN_MULTIVAL_SIZE));
	}

	/**
	 * Constructs a new, empty map, sorted according to the specified comparator.
	 *
	 * @param comparator the comparator that will be used to order this map. If
	 *                   {@code null}, the {@linkplain Comparable natural ordering}
	 *                   of the keys will be used.
	 */
	public Jiffy(Comparator<? super K> comparator) {
		startTime = System.nanoTime();
		this.comparator = comparator;
		initializeHead();
	}

	/**
	 * Constructs a new map containing the same mappings as the given map, sorted
	 * according to the {@linkplain Comparable natural ordering} of the keys.
	 *
	 * @param m the map whose mappings are to be placed in this map
	 * @throws ClassCastException   if the keys in {@code m} are not
	 *                              {@link Comparable}, or are not mutually
	 *                              comparable
	 * @throws NullPointerException if the specified map or any of its keys or
	 *                              values are null
	 */
	public Jiffy(Map<? extends K, ? extends V> m) {
		startTime = System.nanoTime();
		this.comparator = null;
		initializeHead();
		putAll(m);
	}

	/**
	 * Constructs a new map containing the same mappings and using the same ordering
	 * as the specified sorted map.
	 *
	 * @param m the sorted map whose mappings are to be placed in this map, and
	 *          whose comparator is to be used to sort this map
	 * @throws NullPointerException if the specified sorted map or any of its keys
	 *                              or values are null
	 */
	public Jiffy(SortedMap<K, ? extends V> m) {
		startTime = System.nanoTime();
		this.comparator = m.comparator();
		initializeHead();
		buildFromSorted(m); // initializes transients
	}

	/**
	 * Returns a shallow copy of this {@code ConcurrentSkipListMap} instance. (The
	 * keys and values themselves are not cloned.)
	 *
	 * @return a shallow copy of this map
	 */
	public Jiffy<K, V> clone() {
		throw new UnsupportedOperationException();
//        try {
//            @SuppressWarnings("unchecked")
//            ConcurrentSkipListMap<K,V> clone =
//                (ConcurrentSkipListMap<K,V>) super.clone();
//            clone.keySet = null;
//            clone.entrySet = null;
//            clone.values = null;
//            clone.descendingMap = null;
//            clone.adder = null;
//            clone.buildFromSorted(this);
//            return clone;
//        } catch (CloneNotSupportedException e) {
//            throw new InternalError();
//        }
	}

	/**
	 * Streamlined bulk insertion to initialize from elements of given sorted map.
	 * Call only from constructor or clone method.
	 */
	private void buildFromSorted(SortedMap<K, ? extends V> map) {
		throw new UnsupportedOperationException();
//        if (map == null)
//            throw new NullPointerException();
//        Iterator<? extends Map.Entry<? extends K, ? extends V>> it =
//            map.entrySet().iterator();
//
//        /*
//         * Add equally spaced indices at log intervals, using the bits
//         * of count during insertion. The maximum possible resulting
//         * level is less than the number of bits in a long (64). The
//         * preds array tracks the current rightmost node at each
//         * level.
//         */
//        @SuppressWarnings("unchecked")
//        Index<K,V>[] preds = (Index<K,V>[])new Index<?,?>[64];
//        Node<K,V> bp = new Node<K,V>(null, null, null);
//        Index<K,V> h = preds[0] = new Index<K,V>(bp, null, null);
//        long count = 0;
//
//        while (it.hasNext()) {
//            Map.Entry<? extends K, ? extends V> e = it.next();
//            K k = e.getKey();
//            V v = e.getValue();
//            if (k == null || v == null)
//                throw new NullPointerException();
//            Node<K,V> z = new Node<K,V>(k, v, null);
//            bp = bp.next = z;
//            if ((++count & 3L) == 0L) {
//                long m = count >>> 2;
//                int i = 0;
//                Index<K,V> idx = null, q;
//                do {
//                    idx = new Index<K,V>(z, idx, null);
//                    if ((q = preds[i]) == null)
//                        preds[i] = h = new Index<K,V>(h.node, h, idx);
//                    else
//                        preds[i] = q.right = idx;
//                } while (++i < preds.length && ((m >>>= 1) & 1L) != 0L);
//            }
//        }
//        if (count != 0L) {
//            VarHandle.releaseFence(); // emulate volatile stores
//            addCount(count);
//            head = h;
//            VarHandle.fullFence();
//        }
	}

	/* ---------------- Serialization -------------- */

	// removed

	/* ------ Map API methods ------ */

	/**
	 * Returns {@code true} if this map contains a mapping for the specified key.
	 *
	 * @param key key whose presence in this map is to be tested
	 * @return {@code true} if this map contains a mapping for the specified key
	 * @throws ClassCastException   if the specified key cannot be compared with the
	 *                              keys currently in the map
	 * @throws NullPointerException if the specified key is null
	 */
	public boolean containsKey(Object key) {
		return doGet(key, NEWEST_VERSION) != null;
	}

	private boolean containsKey(Object key, long version) {
		return doGet(key, version) != null;
	}

	/**
	 * Returns the value to which the specified key is mapped, or {@code null} if
	 * this map contains no mapping for the key.
	 *
	 * <p>
	 * More formally, if this map contains a mapping from a key {@code k} to a value
	 * {@code v} such that {@code key} compares equal to {@code k} according to the
	 * map's ordering, then this method returns {@code v}; otherwise it returns
	 * {@code null}. (There can be at most one such mapping.)
	 *
	 * @throws ClassCastException   if the specified key cannot be compared with the
	 *                              keys currently in the map
	 * @throws NullPointerException if the specified key is null
	 */
	public V get(Object key) {
		return doGet(key, NEWEST_VERSION);
	}

	private V get(Object key, long version) {
		return doGet(key, version);
	}

	/**
	 * Returns the value to which the specified key is mapped, or the given
	 * defaultValue if this map contains no mapping for the key.
	 *
	 * @param key          the key
	 * @param defaultValue the value to return if this map contains no mapping for
	 *                     the given key
	 * @return the mapping for the key, if present; else the defaultValue
	 * @throws NullPointerException if the specified key is null
	 * @since 1.8
	 */
	public V getOrDefault(Object key, V defaultValue) {
		return getOrDefault(key, defaultValue, NEWEST_VERSION);
	}

	private V getOrDefault(Object key, V defaultValue, long version) {
		V v;
		return (v = doGet(key, version)) == null ? defaultValue : v;
	}

	static ThreadLocal<Long> lastVersion = new ThreadLocal<>() {
		@Override
		protected Long initialValue() {
			return 0l; // (long)
						// CURRENT_VERSION.getAcquire(this);
		}
	};

	/**
	 * Associates the specified value with the specified key in this map. If the map
	 * previously contained a mapping for the key, the old value is replaced.
	 *
	 * @param key   key with which the specified value is to be associated
	 * @param value value to be associated with the specified key
	 * @return the previous value associated with the specified key, or {@code null}
	 *         if there was no mapping for the key
	 * @throws ClassCastException   if the specified key cannot be compared with the
	 *                              keys currently in the map
	 * @throws NullPointerException if the specified key or value is null
	 */
	public V put(K key, V value) {
		if (key == null || value == null)
			throw new NullPointerException();

		long optimisticVersion = getCurrentVersion() + 1;
		Triple<Revision<K, V>, Revision<K, V>, Integer> triple = doPutSingle(key, value, optimisticVersion, false);

		Revision<K, V> revision = triple.first;
		Revision<K, V> next = triple.second;
		Integer indexInNext = triple.third;

		V ret = (next != null && indexInNext >= 0) ? next.getValue().getValueByIndex(indexInNext) : null;

		long finalVersion = helpSingle(revision);
		
		if (next != null) {
			long gcNum = getGcNum(finalVersion);
			doGc(revision, next, gcNum, true);
		}

		return ret;
	}

	private boolean doGc(Revision<K, V> current, Revision<K, V> next, long gcNum, boolean regularNext) {
		long[] statsArray = null;
		if (STATISTICS)
			statsArray = new long[2];

		boolean ret = doGc(current, next, gcNum, regularNext, statsArray);

		if (STATISTICS)
			stats.get().updateGC(statsArray);

		return ret;
	}

	private boolean doGc(Revision<K, V> current, Revision<K, V> next, long gcNum, boolean regularNext,
			long[] statsArray) {
		if (STATISTICS && statsArray != null)
			statsArray[0]++;

		boolean ret = false;

		outer: {
			if (next == null) {
				ret = true;
				break outer;
			}

			if (current.acquireVersion() < 0) {
				assert current.descriptor != null;
				long newVer = current.descriptor.acquireVersion();
				assert newVer >= 0;
				current.setVersion(newVer);
			}

			if (current.effectiveVersion() <= gcNum) {
				ret = doGcInner(current, next, regularNext, statsArray);
				break outer;
			}

			boolean lRet = doGc(next, next.acquireNext(), gcNum, true, statsArray);
			if (next.getType() == Revision.MERGE) {
				lRet &= doGc(next, ((MergeRevision<K, V>) next).acquireRightNext(), gcNum, false, null);
				lRet &= ((MergeRevision<K, V>) next).acquireReadyToGC();
			}

			if (lRet) {
				if ((next.getValue().size() == 0 || current.version == next.version)
						&& (next.descriptor == null || next.descriptor.isFinished())
						&& (current.descriptor == null || current.descriptor.isFinished())) {

					if (regularNext)
						current.nullNext();
					else {
						MergeRevision<K, V> mergeRevision = (MergeRevision<K, V>) current;
						mergeRevision.nullRightNext();
					}

					ret = true;
					break outer;
				}
			}
		}

		return ret;
	}

	private boolean doGcInner(Revision<K, V> previous, Revision<K, V> candidate, boolean regularNext,
			long[] statsArray) {
		if (STATISTICS && statsArray != null)
			statsArray[1]++;

		if (candidate == null)
			return true;

		if (candidate.descriptor != null && !candidate.descriptor.isFinished())
			return false;

		boolean ret = true;
		if (!doGcInner(candidate, candidate.next, true, statsArray))
			ret = false;

		MergeRevision<K, V> mergeRevision = null;
		if (candidate.getType() == Revision.MERGE) {
			mergeRevision = (MergeRevision<K, V>) candidate;
			if (!doGcInner(candidate, mergeRevision.rightNext, false, null) || !mergeRevision.acquireReadyToGC())
				ret = false;
		}

		if (!ret)
			return false;

		if (previous.descriptor != null && !previous.descriptor.isFinished())
			return false;

		if (regularNext)
			previous.nullNext();
		else
			((MergeRevision<K, V>) previous).nullRightNext();

		return true;
	}

	public void put(Batch<K, V> batch) {
		if (batch == null)
			throw new NullPointerException();

		int size = batch.mapSize();
		if (size == 0)
			return;

		if (size == 1) {
			Entry<K, V> first = batch.mapOnlyEntry();

			K key = first.getKey();
			V val = first.getValue();
			V ret;
			if (val != null)
				ret = put(key, val);
			else
				ret = remove(key);
			batch.setSubstitutedValue(key, ret);

			return;
		}

		batch.prepare();

		List<Revision<K, V>> revisions = new ArrayList<>();

		long optimisticVersion = getCurrentVersion() + 1;
		BatchDescriptor<K, V> descriptor = new BatchDescriptor<>(batch, optimisticVersion);

		long finalVersion = helpBatchPrimaryRun(descriptor, batch.lastKey(), revisions);
		descriptor.finish();

		descriptor.batch = null;


		long gcNum = getGcNum(finalVersion);
		for (Revision<K, V> revision : revisions) {
			if (revision.getType() == Revision.MERGE) {
				MergeRevision<K, V> mergeRevision = (MergeRevision<K, V>) revision;
				MergeTerminatorRevision<K, V> terminatorRevision = mergeRevision.mergeTerminator;

				doGc(mergeRevision, mergeRevision.next, gcNum, true);
				doGc(mergeRevision, terminatorRevision.next, gcNum, false);
			} else {
				doGc(revision, revision.next, gcNum, true);
			}
		}
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		throw new UnsupportedOperationException();
	}

	void publishVersion(long finalVersion) {
		if (USE_TSC) {
			int counter = 0;
			while (true) {
				if (finalVersion <= getCurrentVersion())
					break;
				counter++;
				if (counter % 5 == 0)
					Thread.yield();
			}
		} else {

			int i = 0;
			for (i = 0; i < 5; i++) {
				if (finalVersion <= getCurrentVersion()) {
					break;
				}
			}
			if (i == 5)
				CURRENT_VERSION.getAndAdd(this, 1);
		}
	}

	public String debugAll() {
		StringBuilder builder = new StringBuilder();
		builder.append("Map:\n");
		Node<K, V> n = baseHead();
		while (n != null) {
			builder.append(n + "\n");
			n = n.acquireNext();
		}
		return builder.toString();
	}

	public String printAll() {
		StringBuilder builder = new StringBuilder();
		builder.append("Map:\n");
		Node<K, V> n = baseHead();
		while (n != null) {
			Revision<K, V> revision = n.acquireRevisionHead();
			builder.append(String.format("- %s\n", revision.getValue()));
			n = n.acquireNext();
		}
		return builder.toString();
	}

	long helpPut(Revision<K, V> revision) {
		long revisionVersion = revision.effectiveVersion();

		if (revisionVersion > 0)
			return revisionVersion;

		long ret;
		if (revision.descriptor == null)
			ret = helpSingle(revision);
		else
			ret = helpBatch(revision);

		return ret;
	}

	private long helpSingle(Revision<K, V> revision) {
		assert revision.descriptor == null;

		Revision<K, V> nextRevision = revision.acquireNext();
		if (nextRevision != null && nextRevision.effectiveVersion() < 0) {
			assert nextRevision.descriptor == null;
			helpSingle(nextRevision);
		}

		long finalVersion = revision.acquireVersion();

		if (finalVersion > 0)
			return finalVersion;

		int type = revision.getType();
		switch (type) {
		case Revision.SPLIT:
			finalVersion = helpSplit((SplitRevision<K, V>) revision);
			assert revision.acquireVersion() > 0;
			break;
		case Revision.MERGE:
			finalVersion = helpMerge((MergeRevision<K, V>) revision);
			assert revision.acquireVersion() > 0;
			break;
		case Revision.MERGE_TERMINATOR:
			finalVersion = helpMergeTerminator((MergeTerminatorRevision<K, V>) revision);
			break;
		default:
			long currentVersion = getCurrentVersion();
			if (currentVersion < -finalVersion)
				publishVersion(currentVersion);
			finalVersion = revision.trySetVersion(currentVersion);
		}

		return finalVersion;
	}

	private void extractWitnessValues(Batch<K, V> batch, Revision<K, V> revision, BatchDescriptor<K, V> descriptor) {
		MultiValIndices<K> indices = revision.nextIndices;
		MultiVal<K, V> nextMval = revision.next.getValue();

		for (int j = 0; j < indices.indices.length; j++) {
			K key = batch.getKeyByIndex(revision.indexOfLeftmostRelevantBatchKey + j);

			int index = indices.indices[j];
			V value = index < 0 ? null : nextMval.getByIndex(index).getValue();
			batch.setSubstitutedValue(key, value);
		}
	}

	private void retrieveBatchReturnValues2(BatchDescriptor<K, V> descriptor, Revision<K, V> revision) {
		// We can dereference revision.next because batch is not marked as finished, and
		// thus subsequent revisions won't be GCed yet.

		Batch<K, V> batch = descriptor.batch;

		int type = revision.getType();
		// case for merge terminator could be removed
		if (type == Revision.MERGE) {
			MergeRevision<K, V> mergeRevision = (MergeRevision<K, V>) revision;
			MergeTerminatorRevision<K, V> terminatorRevision = mergeRevision.mergeTerminator;

			extractWitnessValues(batch, terminatorRevision, descriptor);
			extractWitnessValues(batch, mergeRevision, descriptor);

			mergeRevision.setReadyToGC();
		} else if (type == Revision.SPLIT) {
			SplitRevision<K, V> leftRevision;
			SplitRevision<K, V> rightRevision;

			SplitRevision<K, V> splitRevision = (SplitRevision<K, V>) revision;
			if (splitRevision.left) {
				leftRevision = splitRevision;
				rightRevision = leftRevision.sibling;
			} else {
				rightRevision = splitRevision;
				leftRevision = rightRevision.sibling;
			}

			extractWitnessValues(batch, leftRevision, descriptor);
		} else if (type == Revision.REGULAR) {
			extractWitnessValues(batch, revision, descriptor);
		} else {
			throw new JiffyInternalException("wrong revision type");
		}
	}

	private long helpBatchPrimaryRun(BatchDescriptor<K, V> descriptor, K firstRelevantKeyFromRight,
			List<Revision<K, V>> revisions) {

		long optimisticVersion = -1 * descriptor.acquireVersion();

		long[] statsArray = null;
		if (STATISTICS)
			statsArray = new long[1];

		int nextKeyIndex = descriptor.batch.size() - 1;
		while (nextKeyIndex >= 0) {
			if (STATISTICS)
				statsArray[0]++;

			Revision<K, V> currentRevision = doPutBatch(descriptor, nextKeyIndex, true);

			if (currentRevision.getType() == Revision.MERGE_TERMINATOR)
				currentRevision = helpBatchRevision(currentRevision, true);

			if (currentRevision.getType() == Revision.SPLIT) {
				SplitRevision<K, V> splitRevision = (SplitRevision<K, V>) currentRevision;
				if (!splitRevision.left) {
					currentRevision = splitRevision.sibling;
					revisions.add(splitRevision);
				}
			}
			revisions.add(currentRevision);

			nextKeyIndex = currentRevision.indexOfLeftmostRelevantBatchKey - 1;
		}

		long currentVersion = getCurrentVersion();
		if (currentVersion < optimisticVersion)
			publishVersion(currentVersion);
		long finalVersion = descriptor.trySetVersion(currentVersion);

		for (var r : revisions) {
			if (r.getType() == Revision.SPLIT && !((SplitRevision<K, V>) r).left)
				continue;

			retrieveBatchReturnValues2(descriptor, r);
		}

		if (STATISTICS)
			stats.get().updateHelpBatchPrimaryRun(statsArray);

		autoscalingLastTimeSet.set(optimisticVersion);

		return finalVersion;
	}

	private long helpBatch(Revision<K, V> revision) {
		long[] statsArray = null;
		if (STATISTICS)
			statsArray = new long[1];

		BatchDescriptor<K, V> descriptor = revision.descriptor;

		Batch<K, V> batch = descriptor.batch;
		if (batch == null)
			return descriptor.acquireVersion();

		boolean setFinalVersion = true;
		long version = 0;

		// for autoscaler only
		long optimisticVersion = descriptor.acquireVersion();
		if (optimisticVersion < 0)
			optimisticVersion *= -1;
		else
			optimisticVersion = 0;

		revision = helpBatchRevision(revision, false);

		if (revision != null) {
			// could be null if we helped a terminator revision but
			// we haven't found the merge revision because it's been already GCd
			
			int nextKeyIndex = revision.indexOfLeftmostRelevantBatchKey - 1;

			while (nextKeyIndex >= 0) {
				version = descriptor.acquireVersion();
				if (version > 0) {
					setFinalVersion = false;
					break;
				}

				if (STATISTICS)
					statsArray[0]++;

				Revision<K, V> currentRevision = doPutBatch(descriptor, nextKeyIndex, false);

				if (currentRevision == null) {
					setFinalVersion = false;
					version = descriptor.acquireVersion();
					break;
				}

				if (currentRevision.getType() == Revision.MERGE_TERMINATOR) {
					if (descriptor.acquireVersion() > 0) {
						setFinalVersion = false;
						break;
					}

					currentRevision = helpBatchRevision(currentRevision, false);

					if (currentRevision == null) {
						setFinalVersion = false;
						break;
					}
				}

				nextKeyIndex = currentRevision.indexOfLeftmostRelevantBatchKey - 1;
			}

			if (setFinalVersion) {
				long currentVersion = getCurrentVersion();
				if (optimisticVersion != 0 && currentVersion < optimisticVersion)
					publishVersion(currentVersion);
				version = descriptor.trySetVersion(currentVersion);
			}
		} else
			version = descriptor.acquireVersion();

		version = descriptor.acquireVersion();

		assert version > 0;

		if (STATISTICS)
			stats.get().updateHelpBatch(statsArray);

		if (optimisticVersion != 0)
			autoscalingLastTimeSet.set(optimisticVersion);

		return version;
	}

	private Revision<K, V> helpBatchRevision(Revision<K, V> revision, boolean primaryRun) {
		Revision<K, V> ret = null;

		int type = revision.getType();

		switch (type) {
		case Revision.REGULAR: {
			ret = revision;
			break;
		}
		case Revision.SPLIT: {
			SplitRevision<K, V> splitRevision = (SplitRevision<K, V>) revision;
			helpSplit(splitRevision);
			ret = splitRevision.left ? splitRevision : splitRevision.sibling;
			break;
		}
		case Revision.MERGE: {
			helpMerge((MergeRevision<K, V>) revision);
			ret = revision;
			break;
		}
		case Revision.MERGE_TERMINATOR:
			MergeTerminatorRevision<K, V> terminator = (MergeTerminatorRevision<K, V>) revision;
			helpMergeTerminator((MergeTerminatorRevision<K, V>) revision);
			MergeRevision<K, V> mergeRevision = null;
			long version = revision.descriptor.acquireVersion();
			if (version < 0 || primaryRun) {
				mergeRevision = findMergeRevisionBatch(terminator, primaryRun);
				if (mergeRevision != null && version < 0)
					helpMerge(mergeRevision);
			}
			ret = mergeRevision;
			break;
		}
		// return leftmostKey;
		return ret;
	}

	/******/

	private Triple<Revision<K, V>, Revision<K, V>, Integer> doRemoveSingle(K key, long optimisticVersion) {
		if (head != null && key == null)
			throw new NullPointerException();

		Triple<Revision<K, V>, Revision<K, V>, Integer> ret = null;

		long[] statsArray = null;
		if (STATISTICS)
			statsArray = new long[10];

		Comparator<? super K> cmp = comparator;
		Revision<K, V> revision = new Revision<K, V>(null, -optimisticVersion, null);

		outer: for (;;) {
			if (STATISTICS)
				statsArray[0]++;
			Index<K, V> h;
			Node<K, V> b;
			VarHandle.acquireFence();
			h = head;
			for (Index<K, V> q = h, r, d;;) { // count while descending
				while ((r = q.right) != null) {
					Node<K, V> p = r.node;
					K k = p != null ? p.key : null;
					if (p == null || p.isTerminated())
						RIGHT.compareAndSet(q, r, r.right);
					else if (cpr(cmp, key, k) > 0)
						q = r;
					else
						break;
				}
				if ((d = q.down) != null)
					q = d;
				else {
					b = q.node;
					break;
				}
			}

			assert b != null; // because we have at least one MultiVal
			Node<K, V> n;
			insertionPoint: for (;;) { // find insertion point
				if (STATISTICS)
					statsArray[1]++;
				K k;
				int c;
				if ((n = b.next) == null) {
					if (b.key == null) // if empty, type check key now
						cpr(cmp, key, key);
					c = -1;
				} else if ((k = n.key) == null)
					continue outer; // can't append; restart
				else if (n.isTerminated()) {
					unlinkNode(b, n);
					c = 1;
				} else if ((c = cpr(cmp, key, k)) >= 0)
					b = n;

				if (c < 0) {
					if (b.getType() == Node.TEMP_SPLIT) {
						if (STATISTICS)
							statsArray[2]++;
						helpTempSplitNode((TempSplitNode<K, V>) b, null);
						continue outer;
					}

					Revision<K, V> head = b.acquireRevisionHead();

					if (b.isTerminated()) {
						if (STATISTICS)
							statsArray[3]++;
						continue outer; // retry
					}

					if (head.effectiveVersion() < 0
							&& (head.descriptor != null || head.getType() != Revision.REGULAR)) {
						if (STATISTICS)
							statsArray[4]++;

						helpPut(head);
						continue insertionPoint;
					}

					if (b.acquireNext() != n) {
						if (STATISTICS)
							statsArray[5]++;
						continue insertionPoint;
					}

					MultiVal<K, V> headMval = head.getValue();
					int index = headMval.indexOfKeyInMultiVal(key, true);
					if (index < 0) {
						if (STATISTICS)
							statsArray[6]++;
						ret = new Triple<>(null, head, index);
						break outer;
					}

					long delta = optimisticVersion - autoscalingLastTimeSet.get();
					double[] newAutoscaleParam = newAutoscaleParamForUpdates(head.getAutoscaleParam(), delta);

					int endSize = headMval.size() - 1;
					if (b == h.node || whatUpdate(endSize, newAutoscaleParam, head) != -1) {
						if (STATISTICS)
							statsArray[7]++;
						// TODO if base node we sometimes can unnecessarily put a new empty revision
						MultiVal<K, V> mval = headMval.remove(key, index);
						revision.value = null;
						revision.setValue(mval, null);
						revision.next = head;

						revision.setAutoscaleParam(newAutoscaleParam);

						if (b.tryPutRevisionSingle(revision)) {
							ret = new Triple<>(revision, head, index);
							break outer;
						}
						continue insertionPoint;
					} else {
						if (STATISTICS)
							statsArray[8]++;
						if (head.effectiveVersion() < 0) {
							if (STATISTICS)
								statsArray[9]++;
							helpPut(head);

							// optimization
							Revision<K, V> newHead = b.acquireRevisionHead();
							if (newHead != head)
								continue insertionPoint;
						}

						MergeTerminatorRevision<K, V> mergeTerminator = new MergeTerminatorRevision<>(revision.version,
								b, head, index);
						mergeTerminator.setAutoscaleParam(newAutoscaleParam);

						if (b.tryPutRevisionSingle(mergeTerminator)) {
							if (SPLIT_MERGE_STATISTICS)
								mergeCounter.set(mergeCounter.get() + 1);

							helpMergeTerminator(mergeTerminator);
							ret = new Triple<>(mergeTerminator, head, index); // not returning the true merge revision!
							break outer;
						}
						// carries on, i.e., continue insertionPoint
					}
				}
			}
		}

		autoscalingLastTimeSet.set(optimisticVersion);

		if (STATISTICS)
			stats.get().updateDoRemoveSingle(statsArray);

		return ret;
	}

	/**
	 * Removes the mapping for the specified key from this map if present.
	 *
	 * @param key key for which mapping should be removed
	 * @return the previous value associated with the specified key, or {@code null}
	 *         if there was no mapping for the key
	 * @throws ClassCastException   if the specified key cannot be compared with the
	 *                              keys currently in the map
	 * @throws NullPointerException if the specified key is null
	 */
	@SuppressWarnings("unchecked")
	public V remove(Object key) {
		// Mostly code from put

		if (key == null)
			throw new NullPointerException();

		K cKey = (K) key;

		long optimisticVersion = getCurrentVersion() + 1;

		Triple<Revision<K, V>, Revision<K, V>, Integer> triple = doRemoveSingle(cKey, optimisticVersion);

		Revision<K, V> revision = triple.first;
		Revision<K, V> next = triple.second;
		Integer indexInNext = triple.third;

		V ret = (next != null && indexInNext >= 0) ? next.getValue().getValueByIndex(indexInNext) : null;

		long finalVersion;
		if (revision == null) {
			finalVersion = helpPut(next);

			if (next.descriptor != null && !next.descriptor.isFinished())
				return ret;

			long gcNum = getGcNum(finalVersion);
			doGc(next, next.acquireNext(), gcNum, true);
		} else if (revision.getType() == Revision.MERGE_TERMINATOR) {
			// A tombstone was already present or we had to put a MergeTerminator
			// and we don't have the true MergeRevision. But the MergeRevision will be
			// with the same optimisticVersion as the MergeTerminatorRevision.
			MergeTerminatorRevision<K, V> terminatorRevision = (MergeTerminatorRevision<K, V>) revision;
			Revision<K, V> terminatorNext = next;

			MergeRevision<K, V> mergeRevision = findMergeRevisionSingle(terminatorRevision, 0);
			mergeRevision.setReadyToGC();

			Revision<K, V> mergeRevisionNext = mergeRevision.next;

			finalVersion = mergeRevision.acquireVersion();

			if (next.descriptor != null && !next.descriptor.isFinished()) {
				return ret;
			}

			long gcNum = getGcNum(finalVersion);
			doGc(mergeRevision, mergeRevisionNext, gcNum, true);
			doGc(mergeRevision, terminatorNext, gcNum, false);
		} else { // a tombstone or a just reduced mval was created
			finalVersion = helpSingle(revision);

			long gcNum = getGcNum(finalVersion);
			doGc(revision, next, gcNum, true);
		}

		return ret;
	}

	/**
	 * Returns {@code true} if this map maps one or more keys to the specified
	 * value. This operation requires time linear in the map size. Additionally, it
	 * is possible for the map to change during execution of this method, in which
	 * case the returned result may be inaccurate.
	 *
	 * @param value value whose presence in this map is to be tested
	 * @return {@code true} if a mapping to {@code value} exists; {@code false}
	 *         otherwise
	 * @throws NullPointerException if the specified value is null
	 */
	public boolean containsValue(Object value) {
		return containsValue(value, NEWEST_VERSION);
	}

	private boolean containsValue(Object value, long version) {
		throw new UnsupportedOperationException();
//		if (value == null)
//			throw new NullPointerException();
//
//		Node<K, V> b;
//		b = baseHead();
//		while (b != null) {
//			// TODO b is not merge terminator or split node
//			if (!b.isTerminated() && containsValue(b, version, value))
//				return true;
//
//			b = b.acquireNext();
//		}
//		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	public int size() {
		return size(NEWEST_VERSION);
	}

	public int size(long version) {
		long count = 0;

		for (var e : entrySet()) {
			count++;
		}

		return count >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) count;
	}

	public int[] NodesStatistics() {
		int nonEmptyNodes = 0;
		int emptyNodes = 0;
		int terminatedNodes = 0;
		int mergeTerminatedNodes = 0;
		int markerNodes = 0;
		int keys = 0;

		VarHandle.acquireFence();
		Node<K, V> base = baseHead();
		Node<K, V> n = base;
		while (n != null) {
			if (n.key == null && n != base)
				markerNodes++;
			else {
//				boolean mergeTerminated = false;
//				Revision<K, V> revision = n.acquireRevisionHead();
//				if (n.isTerminated())
//					terminatedNodes++;
//				else if (revision.getType() == Revision.MERGE_TERMINATOR) {
//					mergeTerminated = true;
//					mergeTerminatedNodes++;
//				}
//
//				// TODO not corrected for merge revisions
//				// revision = retrieveRevision(n, null, NEWEST_VERSION);
//				// revision = null;
//				if (revision == null || revision.getValue().size() == 0)
//					emptyNodes++;
//				else {
//					keys += revision.getValue().size();
//					if (!mergeTerminated)
//						nonEmptyNodes++;
//				}
			}
			n = n.next;
		}
		return new int[] { nonEmptyNodes, emptyNodes, terminatedNodes, mergeTerminatedNodes, markerNodes, keys };
	}

	public String getNodesStatistics() {
		return String.format("estimated keys per node: %.2f (MAX/MIN_MULTIVAL_SIZE: %d/%d)", estimateKeysPerNode(),
				MAX_MULTIVAL_SIZE, MIN_MULTIVAL_SIZE);
			
//		int[] arr = NodesStatistics();
//		String ret = String.format(
//				"nonEmptyNodes: %d, emptyNodes: %d, terminatedNodes: %d, mergeTerminatedNodes: %d, markerNodes: %d, keys: %d, keys/nonEmptyNodes: %.2f\t(MAX/MIN_MULTIVAL_SIZE: %d/%d)",
//				arr[0], arr[1], arr[2], arr[3], arr[4], arr[5], 1f * arr[5] / arr[0], MAX_MULTIVAL_SIZE,
//				MIN_MULTIVAL_SIZE);
//
//		return ret;
	}

	public float estimateKeysPerNode() {
		int testNodes = 10;
		int keys = 0;

		VarHandle.acquireFence();
		Node<K, V> base = baseHead();
		Node<K, V> n = base;
		int nodeCounter = 0;
		while (n != null && nodeCounter < testNodes) {
			if (n == base || n.key != null) {
				Revision<K, V> revision = n.acquireRevisionHead();
				if (!n.isTerminated() && revision.getType() != Revision.MERGE_TERMINATOR) {
					keys += revision.getValue().size();
					nodeCounter++;
				}
			}
			n = n.next;
		}
		return nodeCounter == 0 ? 0 : 1f * keys / nodeCounter;
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean isEmpty() {
		return isEmpty(NEWEST_VERSION);
	}

	public boolean isEmpty(long version) {
		return findFirst(version) == null;
	}

	/**
	 * Removes all of the mappings from this map.
	 */
	public void clear() {
		clear(NEWEST_VERSION);
	}

	private void clear(long version) {
		// TODO maybe clear should remove all versions older than some version
		throw new UnsupportedOperationException();
//		Index<K, V> h, r, d;
//		Node<K, V> b;
//		VarHandle.acquireFence();
//		while ((h = head) != null) {
//			if ((r = h.right) != null) // remove indices
//				RIGHT.compareAndSet(h, r, null);
//			else if ((d = h.down) != null) // remove levels
//				HEAD.compareAndSet(this, h, d);
//			else {
//				long count = 0L;
//				if ((b = h.node) != null) { // remove nodes
//					Node<K, V> n;
//					V v;
//					while ((n = b.next) != null) {
//						// if ((v = n.val) != null && VAL.compareAndSet(n, v, null)) {
//						Revision<K,V> head = n.acquireRevisionHead();
//						if (!n.isTerminated()) {
//							n.terminate();
//							--count;
//							v = null;
//						}
//						if (v == null)
//							unlinkNode(b, n);
//					}
//				}
//				if (count != 0L)
//					addCount(count);
//				else
//					break;
//			}
//		}
	}

	public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
		throw new UnsupportedOperationException();
	}

	public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
		throw new UnsupportedOperationException();
	}

	public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
		throw new UnsupportedOperationException();
	}

	public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
		throw new UnsupportedOperationException();
	}

	/* ---------------- View methods -------------- */

	/*
	 * Note: Lazy initialization works for views because view classes are
	 * stateless/immutable so it doesn't matter wrt correctness if more than one is
	 * created (which will only rarely happen). Even so, the following idiom
	 * conservatively ensures that the method returns the one it created if it does
	 * so, not one created by another racing thread.
	 */

	/**
	 * Returns a {@link NavigableSet} view of the keys contained in this map.
	 *
	 * <p>
	 * The set's iterator returns the keys in ascending order. The set's spliterator
	 * additionally reports {@link Spliterator#CONCURRENT},
	 * {@link Spliterator#NONNULL}, {@link Spliterator#SORTED} and
	 * {@link Spliterator#ORDERED}, with an encounter order that is ascending key
	 * order.
	 *
	 * <p>
	 * The {@linkplain Spliterator#getComparator() spliterator's comparator} is
	 * {@code null} if the {@linkplain #comparator() map's comparator} is
	 * {@code null}. Otherwise, the spliterator's comparator is the same as or
	 * imposes the same total ordering as the map's comparator.
	 *
	 * <p>
	 * The set is backed by the map, so changes to the map are reflected in the set,
	 * and vice-versa. The set supports element removal, which removes the
	 * corresponding mapping from the map, via the {@code Iterator.remove},
	 * {@code Set.remove}, {@code removeAll}, {@code retainAll}, and {@code clear}
	 * operations. It does not support the {@code add} or {@code addAll} operations.
	 *
	 * <p>
	 * The view's iterators and spliterators are
	 * <a href="package-summary.html#Weakly"><i>weakly consistent</i></a>.
	 *
	 * <p>
	 * This method is equivalent to method {@code navigableKeySet}.
	 *
	 * @return a navigable set view of the keys in this map
	 */
	public NavigableSet<K> keySet() {
		return new KeySet<>(this);
	}

	public NavigableSet<K> navigableKeySet() {
		return new KeySet<>(this);
	}

	/**
	 * Returns a {@link Collection} view of the values contained in this map.
	 * <p>
	 * The collection's iterator returns the values in ascending order of the
	 * corresponding keys. The collections's spliterator additionally reports
	 * {@link Spliterator#CONCURRENT}, {@link Spliterator#NONNULL} and
	 * {@link Spliterator#ORDERED}, with an encounter order that is ascending order
	 * of the corresponding keys.
	 *
	 * <p>
	 * The collection is backed by the map, so changes to the map are reflected in
	 * the collection, and vice-versa. The collection supports element removal,
	 * which removes the corresponding mapping from the map, via the
	 * {@code Iterator.remove}, {@code Collection.remove}, {@code removeAll},
	 * {@code retainAll} and {@code clear} operations. It does not support the
	 * {@code add} or {@code addAll} operations.
	 *
	 * <p>
	 * The view's iterators and spliterators are
	 * <a href="package-summary.html#Weakly"><i>weakly consistent</i></a>.
	 */
	public Collection<V> values() {
		return new Values<>(this);
	}

	/**
	 * Returns a {@link Set} view of the mappings contained in this map.
	 *
	 * <p>
	 * The set's iterator returns the entries in ascending key order. The set's
	 * spliterator additionally reports {@link Spliterator#CONCURRENT},
	 * {@link Spliterator#NONNULL}, {@link Spliterator#SORTED} and
	 * {@link Spliterator#ORDERED}, with an encounter order that is ascending key
	 * order.
	 *
	 * <p>
	 * The set is backed by the map, so changes to the map are reflected in the set,
	 * and vice-versa. The set supports element removal, which removes the
	 * corresponding mapping from the map, via the {@code Iterator.remove},
	 * {@code Set.remove}, {@code removeAll}, {@code retainAll} and {@code clear}
	 * operations. It does not support the {@code add} or {@code addAll} operations.
	 *
	 * <p>
	 * The view's iterators and spliterators are
	 * <a href="package-summary.html#Weakly"><i>weakly consistent</i></a>.
	 *
	 * <p>
	 * The {@code Map.Entry} elements traversed by the {@code iterator} or
	 * {@code spliterator} do <em>not</em> support the {@code setValue} operation.
	 *
	 * @return a set view of the mappings contained in this map, sorted in ascending
	 *         key order
	 */
	public Set<Map.Entry<K, V>> entrySet() {
		return new EntrySet<K, V>(this);
	}

	public MultiversionNavigableMap<K, V> descendingMap() {
		return new SubMap<K, V>(this, null, false, null, false, true, null);
	}

	public NavigableSet<K> descendingKeySet() {
		return descendingMap().navigableKeySet();
	}

	/* ---------------- AbstractMap Overrides -------------- */

	/**
	 * Compares the specified object with this map for equality. Returns
	 * {@code true} if the given object is also a map and the two maps represent the
	 * same mappings. More formally, two maps {@code m1} and {@code m2} represent
	 * the same mappings if {@code m1.entrySet().equals(m2.entrySet())}. This
	 * operation may return misleading results if either map is concurrently
	 * modified during execution of this method.
	 *
	 * @param o object to be compared for equality with this map
	 * @return {@code true} if the specified object is equal to this map
	 */
	// TODO 
	public boolean equals(Object o) {
		if (o == this)
			return true;
		if (!(o instanceof Map))
			return false;
		return false;
//        Map<?,?> m = (Map<?,?>) o;
//        try {
//            Comparator<? super K> cmp = comparator;
//            // See JDK-8223553 for Iterator type wildcard rationale
//            Iterator<? extends Map.Entry<?,?>> it = m.entrySet().iterator();
//            if (m instanceof SortedMap &&
//                ((SortedMap<?,?>)m).comparator() == cmp) {
//                Node<K,V> b, n;
//                if ((b = baseHead()) != null) {
//                    while ((n = b.next) != null) {
//                        K k; V v;
//                        if ((v = n.val) != null && (k = n.key) != null) {
//                            if (!it.hasNext())
//                                return false;
//                            Map.Entry<?,?> e = it.next();
//                            Object mk = e.getKey();
//                            Object mv = e.getValue();
//                            if (mk == null || mv == null)
//                                return false;
//                            try {
//                                if (cpr(cmp, k, mk) != 0)
//                                    return false;
//                            } catch (ClassCastException cce) {
//                                return false;
//                            }
//                            if (!mv.equals(v))
//                                return false;
//                        }
//                        b = n;
//                    }
//                }
//                return !it.hasNext();
//            }
//            else {
//                while (it.hasNext()) {
//                    V v;
//                    Map.Entry<?,?> e = it.next();
//                    Object mk = e.getKey();
//                    Object mv = e.getValue();
//                    if (mk == null || mv == null ||
//                        (v = get(mk)) == null || !v.equals(mv))
//                        return false;
//                }
//                Node<K,V> b, n;
//                if ((b = baseHead()) != null) {
//                    K k; V v; Object mv;
//                    while ((n = b.next) != null) {
//                        if ((v = n.val) != null && (k = n.key) != null &&
//                            ((mv = m.get(k)) == null || !mv.equals(v)))
//                            return false;
//                        b = n;
//                    }
//                }
//                return true;
//            }
//        } catch (ClassCastException | NullPointerException unused) {
//            return false;
//        }
	}

	/* ------ ConcurrentMap API methods ------ */

	/**
	 * {@inheritDoc}
	 *
	 * @return the previous value associated with the specified key, or {@code null}
	 *         if there was no mapping for the key
	 * @throws ClassCastException   if the specified key cannot be compared with the
	 *                              keys currently in the map
	 * @throws NullPointerException if the specified key or value is null
	 */
	public V putIfAbsent(K key, V value) {
		throw new UnsupportedOperationException();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @throws ClassCastException   if the specified key cannot be compared with the
	 *                              keys currently in the map
	 * @throws NullPointerException if the specified key is null
	 */
	public boolean remove(Object key, Object value) {
		throw new UnsupportedOperationException();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @throws ClassCastException   if the specified key cannot be compared with the
	 *                              keys currently in the map
	 * @throws NullPointerException if any of the arguments are null
	 */
	public boolean replace(K key, V oldValue, V newValue) {
		throw new UnsupportedOperationException();
//        if (key == null || oldValue == null || newValue == null)
//            throw new NullPointerException();
//        for (;;) {
//            Node<K,V> n; V v;
//            if ((n = findNode(key)) == null)
//                return false;
//            if ((v = n.val) != null) {
//                if (!oldValue.equals(v))
//                    return false;
//                if (VAL.compareAndSet(n, v, newValue))
//                    return true;
//            }
//        }
	}

	/**
	 * {@inheritDoc}
	 *
	 * @return the previous value associated with the specified key, or {@code null}
	 *         if there was no mapping for the key
	 * @throws ClassCastException   if the specified key cannot be compared with the
	 *                              keys currently in the map
	 * @throws NullPointerException if the specified key or value is null
	 */
	public V replace(K key, V value) {
		throw new UnsupportedOperationException();
//        if (key == null || value == null)
//            throw new NullPointerException();
//        for (;;) {
//            Node<K,V> n; V v;
//            if ((n = findNode(key)) == null)
//                return null;
//            if ((v = n.val) != null && VAL.compareAndSet(n, v, value))
//                return v;
//        }
	}

	/* ------ SortedMap API methods ------ */

	public Comparator<? super K> comparator() {
		return comparator;
	}

	/**
	 * @throws NoSuchElementException {@inheritDoc}
	 */
	public K firstKey() {
		return firstKey(NEWEST_VERSION);
	}

	private K firstKey(long version) {
		Quad<Node<K, V>, Revision<K, V>, Integer, Node<K, V>> t = findFirst(version);
		if (t == null)
			throw new NoSuchElementException();
		return t.second.getValue().firstKey();
	}

	/**
	 * @throws NoSuchElementException {@inheritDoc}
	 */
	public K lastKey() {
		return lastKey(NEWEST_VERSION);
	}

	public K lastKey(long version) {
		Quad<Node<K, V>, Revision<K, V>, Integer, Node<K, V>> t = findLast(version);
		if (t == null)
			throw new NoSuchElementException();
		return t.second.getValue().lastKey();
	}

	public MultiversionNavigableMap<K, V> subMap() {
		return new SubMap<K, V>(this, null, false, null, false, false, null);
	}

	/**
	 * @throws ClassCastException       {@inheritDoc}
	 * @throws NullPointerException     if {@code fromKey} or {@code toKey} is null
	 * @throws IllegalArgumentException {@inheritDoc}
	 */
	public MultiversionNavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
		if (fromKey == null || toKey == null)
			throw new NullPointerException();
		return new SubMap<K, V>(this, fromKey, fromInclusive, toKey, toInclusive, false, null);
	}

	/**
	 * @throws ClassCastException       {@inheritDoc}
	 * @throws NullPointerException     if {@code toKey} is null
	 * @throws IllegalArgumentException {@inheritDoc}
	 */
	public MultiversionNavigableMap<K, V> headMap(K toKey, boolean inclusive) {
		if (toKey == null)
			throw new NullPointerException();
		return new SubMap<K, V>(this, null, false, toKey, inclusive, false, null);
	}

	/**
	 * @throws ClassCastException       {@inheritDoc}
	 * @throws NullPointerException     if {@code fromKey} is null
	 * @throws IllegalArgumentException {@inheritDoc}
	 */
	public MultiversionNavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
		if (fromKey == null)
			throw new NullPointerException();
		return new SubMap<K, V>(this, fromKey, inclusive, null, false, false, null);
	}

	/**
	 * @throws ClassCastException       {@inheritDoc}
	 * @throws NullPointerException     if {@code fromKey} or {@code toKey} is null
	 * @throws IllegalArgumentException {@inheritDoc}
	 */
	public MultiversionNavigableMap<K, V> subMap(K fromKey, K toKey) {
		return subMap(fromKey, true, toKey, false);
	}

	/**
	 * @throws ClassCastException       {@inheritDoc}
	 * @throws NullPointerException     if {@code toKey} is null
	 * @throws IllegalArgumentException {@inheritDoc}
	 */
	public MultiversionNavigableMap<K, V> headMap(K toKey) {
		return headMap(toKey, false);
	}

	/**
	 * @throws ClassCastException       {@inheritDoc}
	 * @throws NullPointerException     if {@code fromKey} is null
	 * @throws IllegalArgumentException {@inheritDoc}
	 */
	public MultiversionNavigableMap<K, V> tailMap(K fromKey) {
		return tailMap(fromKey, true);
	}

	/* ---------------- Relational operations -------------- */

	/**
	 * Returns a key-value mapping associated with the greatest key strictly less
	 * than the given key, or {@code null} if there is no such key. The returned
	 * entry does <em>not</em> support the {@code Entry.setValue} method.
	 *
	 * @throws ClassCastException   {@inheritDoc}
	 * @throws NullPointerException if the specified key is null
	 */
	public Map.Entry<K, V> lowerEntry(K key) {
		return lowerEntry(key, NEWEST_VERSION);
	}

	private Map.Entry<K, V> lowerEntry(K key, long version) {
		return findNearEntry(key, LT, comparator, version);
	}

	/**
	 * @throws ClassCastException   {@inheritDoc}
	 * @throws NullPointerException if the specified key is null
	 */
	public K lowerKey(K key) {
		return lowerKey(key, NEWEST_VERSION);
	}

	private K lowerKey(K key, long version) {
		Quad<Node<K, V>, Revision<K, V>, Integer, Node<K, V>> t = findNear(key, LT, comparator, version);
		return (t == null) ? null : t.second.getValue().getKeyByIndex(t.third);
	}

	/**
	 * Returns a key-value mapping associated with the greatest key less than or
	 * equal to the given key, or {@code null} if there is no such key. The returned
	 * entry does <em>not</em> support the {@code Entry.setValue} method.
	 *
	 * @param key the key
	 * @throws ClassCastException   {@inheritDoc}
	 * @throws NullPointerException if the specified key is null
	 */
	public Map.Entry<K, V> floorEntry(K key) {
		return floorEntry(key, NEWEST_VERSION);
	}

	private Map.Entry<K, V> floorEntry(K key, long version) {
		return findNearEntry(key, LT | EQ, comparator, version);
	}

	/**
	 * @param key the key
	 * @throws ClassCastException   {@inheritDoc}
	 * @throws NullPointerException if the specified key is null
	 */
	public K floorKey(K key) {
		return floorKey(key, NEWEST_VERSION);
	}

	private K floorKey(K key, long version) {
		Quad<Node<K, V>, Revision<K, V>, Integer, Node<K, V>> t = findNear(key, LT | EQ, comparator, version);
		return (t == null) ? null : t.second.getValue().getKeyByIndex(t.third);
	}

	/**
	 * Returns a key-value mapping associated with the least key greater than or
	 * equal to the given key, or {@code null} if there is no such entry. The
	 * returned entry does <em>not</em> support the {@code Entry.setValue} method.
	 *
	 * @throws ClassCastException   {@inheritDoc}
	 * @throws NullPointerException if the specified key is null
	 */
	public Map.Entry<K, V> ceilingEntry(K key) {
		return ceilingEntry(key, NEWEST_VERSION);
	}

	private Map.Entry<K, V> ceilingEntry(K key, long version) {
		return findNearEntry(key, GT | EQ, comparator, version);
	}

	/**
	 * @throws ClassCastException   {@inheritDoc}
	 * @throws NullPointerException if the specified key is null
	 */
	public K ceilingKey(K key) {
		return ceilingKey(key, NEWEST_VERSION);
	}

	private K ceilingKey(K key, long version) {
		Quad<Node<K, V>, Revision<K, V>, Integer, Node<K, V>> t = findNear(key, GT | EQ, comparator, version);
		return (t == null) ? null : t.second.getValue().getKeyByIndex(t.third);
	}

	/**
	 * Returns a key-value mapping associated with the least key strictly greater
	 * than the given key, or {@code null} if there is no such key. The returned
	 * entry does <em>not</em> support the {@code Entry.setValue} method.
	 *
	 * @param key the key
	 * @throws ClassCastException   {@inheritDoc}
	 * @throws NullPointerException if the specified key is null
	 */
	public Map.Entry<K, V> higherEntry(K key) {
		return higherEntry(key, NEWEST_VERSION);
	}

	private Map.Entry<K, V> higherEntry(K key, long version) {
		return findNearEntry(key, GT, comparator, version);
	}

	/**
	 * @param key the key
	 * @throws ClassCastException   {@inheritDoc}
	 * @throws NullPointerException if the specified key is null
	 */
	public K higherKey(K key) {
		return higherKey(key, NEWEST_VERSION);
	}

	private K higherKey(K key, long version) {
		Quad<Node<K, V>, Revision<K, V>, Integer, Node<K, V>> t = findNear(key, GT, comparator, version);
		return (t == null) ? null : t.second.getValue().getKeyByIndex(t.third);
	}

	/**
	 * Returns a key-value mapping associated with the least key in this map, or
	 * {@code null} if the map is empty. The returned entry does <em>not</em>
	 * support the {@code Entry.setValue} method.
	 */
	public Map.Entry<K, V> firstEntry() {
		return firstEntry(NEWEST_VERSION);
	}

	private Map.Entry<K, V> firstEntry(long version) {
		return findFirstEntry(version);
	}

	/**
	 * Returns a key-value mapping associated with the greatest key in this map, or
	 * {@code null} if the map is empty. The returned entry does <em>not</em>
	 * support the {@code Entry.setValue} method.
	 */
	public Map.Entry<K, V> lastEntry() {
		return lastEntry(NEWEST_VERSION);
	}

	private Map.Entry<K, V> lastEntry(long version) {
		return findLastEntry(version);
	}

	/**
	 * Removes and returns a key-value mapping associated with the least key in this
	 * map, or {@code null} if the map is empty. The returned entry does
	 * <em>not</em> support the {@code Entry.setValue} method.
	 */
	public Map.Entry<K, V> pollFirstEntry() {
		return pollFirstEntry(NEWEST_VERSION);
	}

	private Map.Entry<K, V> pollFirstEntry(long version) {
		return doRemoveFirstEntry(version);
	}

	/**
	 * Removes and returns a key-value mapping associated with the greatest key in
	 * this map, or {@code null} if the map is empty. The returned entry does
	 * <em>not</em> support the {@code Entry.setValue} method.
	 */
	public Map.Entry<K, V> pollLastEntry() {
		return pollLastEntry(NEWEST_VERSION);
	}

	private Map.Entry<K, V> pollLastEntry(long version) {
		return doRemoveLastEntry(version);
	}

	/* ---------------- Strings -------------- */

	@Override
	public String toString() {
		// return toString((long) CURRENT_VERSION.getAcquire(this));
		return toString(NEWEST_VERSION);
	}

	private String toString(long version) {
		StringBuilder builder = new StringBuilder();
		builder.append("{");

		MultiversionNavigableMapSnapshot<K, V> snapshot = snapshot(version);
		boolean addedAnything = false;
		for (var e : snapshot.entrySet()) {
			builder.append(e.getKey().toString() + "=" + e.getValue().toString() + ", ");
			addedAnything = true;
		}
		snapshot.close();

		if (addedAnything)
			builder.replace(builder.length() - 2, builder.length(), "");
		builder.append("}");
		return builder.toString();
	}

	/* ---------------- Iterators -------------- */

	/**
	 * Base of iterator classes
	 */
	abstract class Iter<T> implements Iterator<T> {
		Iterator<Map.Entry<K, V>> iterator;
		Map.Entry<K, V> nextEntry = null;

		Iter() {
			MultiversionNavigableMap<K, V> submap = subMap();
			iterator = submap.entrySet().iterator();
			advance();
		}

		public final boolean hasNext() {
			return nextEntry != null;
		}

		final void advance() {
			if (!iterator.hasNext()) {
				nextEntry = null;
				return;
			}

			nextEntry = iterator.next();
		}

		public final void remove() {
			throw new UnsupportedOperationException();
//			Node<K, V> n;
//			K k;
//			if ((n = lastReturned) == null || (k = n.key) == null)
//				throw new IllegalStateException();
//			// It would not be worth all of the overhead to directly
//			// unlink from here. Using remove is fast enough.
//			// ConcurrentSkipListMap.this.remove(k);
//			MvSkipListMap.this.remove(k);
//			lastReturned = null;
		}
	}

	final class ValueIterator extends Iter<V> {
		ValueIterator(Jiffy<K, V> mvSkipListMap) {
			mvSkipListMap.super();
		}

		public V next() {
			var np = nextEntry;
			if (np == null)
				throw new NoSuchElementException();
			advance();
			return np.getValue();
		}
	}

	final class KeyIterator extends Iter<K> {
		KeyIterator(Jiffy<K, V> mvSkipListMap) {
			mvSkipListMap.super();
		}

		public K next() {
			var np = nextEntry;
			if (np == null)
				throw new NoSuchElementException();
			advance();
			return np.getKey();
		}
	}

	final class EntryIterator extends Iter<Map.Entry<K, V>> {
		EntryIterator(Jiffy<K, V> mvSkipListMap) {
			mvSkipListMap.super();
		}

		public Map.Entry<K, V> next() {
			var ne = nextEntry;
			if (ne == null)
				throw new NoSuchElementException();
			advance();
			return ne;
		}
	}

	/* ---------------- View Classes -------------- */

	/*
	 * View classes are static, delegating to a ConcurrentNavigableMap to allow use
	 * by SubMaps, which outweighs the ugliness of needing type-tests for Iterator
	 * methods.
	 */

	static final <E> List<E> toList(Collection<E> c) {
		// Using size() here would be a pessimization.
		ArrayList<E> list = new ArrayList<E>();
		for (E e : c)
			list.add(e);
		return list;
	}

	static final class KeySet<K, V> extends AbstractSet<K> implements NavigableSet<K> {
		final MultiversionNavigableMap<K, V> m;
		SubMap<K, V> submap = null;

		KeySet(MultiversionNavigableMap<K, V> map) {
			if (map instanceof Jiffy)
				m = (Jiffy<K, V>) map;
			else {
				submap = (SubMap<K, V>) map;
				m = submap;
			}
		}

		public int size() {
			return m.size();
		}

		public boolean isEmpty() {
			return m.isEmpty();
		}

		public boolean contains(Object o) {
			return m.containsKey(o);
		}

		public boolean remove(Object o) {
			throw new UnsupportedOperationException();
			// return m.remove(o, version) != null;
		}

		public void clear() {
			throw new UnsupportedOperationException();
			// m.clear();
		}

		public K lower(K e) {
			return m.lowerKey(e);
		}

		public K floor(K e) {
			return m.floorKey(e);
		}

		public K ceiling(K e) {
			return m.ceilingKey(e);
		}

		public K higher(K e) {
			return m.higherKey(e);
		}

		public Comparator<? super K> comparator() {
			return m.comparator();
		}

		public K first() {
			return m.firstKey();
		}

		public K last() {
			return m.lastKey();
		}

		public K pollFirst() {
			Map.Entry<K, V> e = m.pollFirstEntry();
			return (e == null) ? null : e.getKey();
		}

		public K pollLast() {
			Map.Entry<K, V> e = m.pollLastEntry();
			return (e == null) ? null : e.getKey();
		}

		public Iterator<K> iterator() {
			if (m instanceof Jiffy<?, ?>) {
				Jiffy<K, V> map = (Jiffy<K, V>) m;
				return map.new KeyIterator(map);
			} else
				return ((SubMap<K, V>) m).new SubMapKeyIterator();
		}

		public boolean equals(Object o) {
			if (o == this)
				return true;
			if (!(o instanceof Set))
				return false;
			Collection<?> c = (Collection<?>) o;
			try {
				return containsAll(c) && c.containsAll(this);
			} catch (ClassCastException | NullPointerException unused) {
				return false;
			}
		}

		public Object[] toArray() {
			return toList(this).toArray();
		}

		public <T> T[] toArray(T[] a) {
			return toList(this).toArray(a);
		}

		public Iterator<K> descendingIterator() {
			return descendingSet().iterator();
		}

		public NavigableSet<K> subSet(K fromElement, boolean fromInclusive, K toElement, boolean toInclusive) {
			return new KeySet<>(m.subMap(fromElement, fromInclusive, toElement, toInclusive));
		}

		public NavigableSet<K> headSet(K toElement, boolean inclusive) {
			return new KeySet<>((MultiversionNavigableMap<K, V>) m.headMap(toElement, inclusive));
		}

		public NavigableSet<K> tailSet(K fromElement, boolean inclusive) {
			return new KeySet<>((MultiversionNavigableMap<K, V>) m.tailMap(fromElement, inclusive));
		}

		public NavigableSet<K> subSet(K fromElement, K toElement) {
			return subSet(fromElement, true, toElement, false);
		}

		public NavigableSet<K> headSet(K toElement) {
			return headSet(toElement, false);
		}

		public NavigableSet<K> tailSet(K fromElement) {
			return tailSet(fromElement, true);
		}

		public NavigableSet<K> descendingSet() {
			return new KeySet<>((MultiversionNavigableMap<K, V>) m.descendingMap());
		}

		public Spliterator<K> spliterator() {
			throw new UnsupportedOperationException();
		}
	}

	static final class Values<K, V> extends AbstractCollection<V> {
		final MultiversionNavigableMap<K, V> m;

		Values(MultiversionNavigableMap<K, V> map) {
			m = map;
		}

		public Iterator<V> iterator() {
			if (m instanceof Jiffy) {
				Jiffy<K, V> map = (Jiffy<K, V>) m;
				return map.new ValueIterator(map);
			} else
				return ((SubMap<K, V>) m).new SubMapValueIterator();
		}

		public int size() {
			return m.size();
		}

		public boolean isEmpty() {
			return m.isEmpty();
		}

		public boolean contains(Object o) {
			return m.containsValue(o);
		}

		public void clear() {
			m.clear();
		}

		public Object[] toArray() {
			return toList(this).toArray();
		}

		public <T> T[] toArray(T[] a) {
			return toList(this).toArray(a);
		}

		public Spliterator<V> spliterator() {
			throw new UnsupportedOperationException();
//            return (m instanceof MvSkipListMap<?, ?>)
//                ? ((MvSkipListMap<K,V>)m).valueSpliterator()
//                : ((SubMap<K,V>)m).new SubMapValueIterator();
		}

		public boolean removeIf(Predicate<? super V> filter) {
			throw new UnsupportedOperationException();
//            if (filter == null) throw new NullPointerException();
//            if (m instanceof ConcurrentSkipListMap)
//                return ((ConcurrentSkipListMap<K,V>)m).removeValueIf(filter);
//            // else use iterator
//            Iterator<Map.Entry<K,V>> it =
//                ((SubMap<K,V>)m).new SubMapEntryIterator();
//            boolean removed = false;
//            while (it.hasNext()) {
//                Map.Entry<K,V> e = it.next();
//                V v = e.getValue();
//                if (filter.test(v) && m.remove(e.getKey(), v))
//                    removed = true;
//            }
//            return removed;
		}
	}

	static final class EntrySet<K, V> extends AbstractSet<Map.Entry<K, V>> {
		final Jiffy<K, V> m;
		SubMap<K, V> submap = null;

		EntrySet(MultiversionNavigableMap<K, V> map) {
			if (map instanceof Jiffy)
				m = (Jiffy<K, V>) map;
			else {
				submap = (SubMap<K, V>) map;
				m = submap.m;
			}
		}

		public Iterator<Map.Entry<K, V>> iterator() {
			return submap != null ? submap.new SubMapEntryIterator() : m.new EntryIterator(m);
		}

		public boolean contains(Object o) {
			if (!(o instanceof Map.Entry))
				return false;
			Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
			V v = m.get(e.getKey());
			return v != null && v.equals(e.getValue());
		}

		public boolean remove(Object o) {
			throw new UnsupportedOperationException();
//			if (!(o instanceof Map.Entry))
//				return false;
//			Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
//			return m.remove(e.getKey(), e.getValue());
		}

		public boolean isEmpty() {
			return m.isEmpty();
		}

		public int size() {
			return m.size();
		}

		public void clear() {
			throw new UnsupportedOperationException();
			// m.clear(version);
		}

		public boolean equals(Object o) {
			if (o == this)
				return true;
			if (!(o instanceof Set))
				return false;
			Collection<?> c = (Collection<?>) o;
			try {
				return containsAll(c) && c.containsAll(this);
			} catch (ClassCastException | NullPointerException unused) {
				return false;
			}
		}

//		public Object[] toArray() {
//			return toList(this).toArray();
//		}
//
//		public <T> T[] toArray(T[] a) {
//			return toList(this).toArray(a);
//		}
//
//		public Spliterator<Map.Entry<K, V>> spliterator() {
//			return (m instanceof ConcurrentSkipListMap) ? ((ConcurrentSkipListMap<K, V>) m).entrySpliterator()
//					: ((SubMap<K, V>) m).new SubMapEntryIterator();
//		}

//		public boolean removeIf(Predicate<? super Entry<K, V>> filter) {
//			if (filter == null)
//				throw new NullPointerException();
//			if (m instanceof ConcurrentSkipListMap)
//				return ((ConcurrentSkipListMap<K, V>) m).removeEntryIf(filter);
//			// else use iterator
//			Iterator<Map.Entry<K, V>> it = ((SubMap<K, V>) m).new SubMapEntryIterator();
//			boolean removed = false;
//			while (it.hasNext()) {
//				Map.Entry<K, V> e = it.next();
//				if (filter.test(e) && m.remove(e.getKey(), e.getValue()))
//					removed = true;
//			}
//			return removed;
//		}
	}

	/**
	 * Submaps returned by {@link ConcurrentSkipListMap} submap operations represent
	 * a subrange of mappings of their underlying maps. Instances of this class
	 * support all methods of their underlying maps, differing in that mappings
	 * outside their range are ignored, and attempts to add mappings outside their
	 * ranges result in {@link IllegalArgumentException}. Instances of this class
	 * are constructed only using the {@code subMap}, {@code headMap}, and
	 * {@code tailMap} methods of their underlying maps.
	 *
	 * @serial include
	 */
	// TODO debug public, should be private
	public static final class SubMap<K, V> implements MultiversionNavigableMapSnapshot<K, V> {
		/** Underlying map */
		private final Jiffy<K, V> m;
		/** lower bound key, or null if from start */
		private final K lo;
		/** upper bound key, or null if to end */
		private final K hi;
		/** inclusion flag for lo */
		private final boolean loInclusive;
		/** inclusion flag for hi */
		private final boolean hiInclusive;
		/** direction */
		private final boolean isDescending;

		private SubMap<K, V> mySnapshot;
		@SuppressWarnings("unused")
		private SubMap<K, V> nextSnapshot; // only used if mySnapshot == this
		private long version; // only used if mySnapshot == this
		private boolean closed = false; // only set to true if mySnapshot == this

		/**
		 * Creates a new submap, initializing all fields.
		 */
		private SubMap(Jiffy<K, V> map, K fromKey, boolean fromInclusive, K toKey, boolean toInclusive,
				boolean isDescending) {
			Comparator<? super K> cmp = map.comparator();
			if (fromKey != null && toKey != null && cpr(cmp, fromKey, toKey) > 0)
				throw new IllegalArgumentException("inconsistent range");
			this.m = map;

			this.lo = fromKey;
			this.hi = toKey;
			this.loInclusive = fromInclusive;
			this.hiInclusive = toInclusive;
			this.isDescending = isDescending;
		}

		// new snapshot with specified version, the caller is responsible to register the snapshot
		SubMap(Jiffy<K, V> map, K fromKey, boolean fromInclusive, K toKey, boolean toInclusive, boolean isDescending,
				long version) {
			this(map, fromKey, fromInclusive, toKey, toInclusive, isDescending);
			this.version = version;
			this.mySnapshot = this;
		}

		// new submap view of a snapshot, or of the main map (snapshot == null)
		SubMap(Jiffy<K, V> map, K fromKey, boolean fromInclusive, K toKey, boolean toInclusive, boolean isDescending,
				SubMap<K, V> snapshot) {
			this(map, fromKey, fromInclusive, toKey, toInclusive, isDescending);
			this.mySnapshot = snapshot;
		}

		private static final VarHandle VERSION;
		private static final VarHandle NEXT;
		static {
			try {
				MethodHandles.Lookup l = MethodHandles.lookup();
				VERSION = l.findVarHandle(SubMap.class, "version", long.class);
				NEXT = l.findVarHandle(SubMap.class, "nextSnapshot", SubMap.class);
			} catch (ReflectiveOperationException e) {
				throw new ExceptionInInitializerError(e);
			}
		}

		void setVersion(long version) {
			VERSION.setRelease(this, version);
		}

		long acquireVersion() {
			return (long) VERSION.getAcquire(this);
		}

		// public only for debug
		public long effectiveVersion() {
			if (mySnapshot == null)
				return NEWEST_VERSION;
			return mySnapshot.version;
		}

		SubMap<K, V> acquireNextSnapshot() {
			return (SubMap<K, V>) NEXT.getAcquire(this);
		}

		boolean casNextSnapshot(SubMap<K, V> expected, SubMap<K, V> value) {
			return NEXT.compareAndSet(this, expected, value);
		}

		private void checkSnapshot() {
			if (mySnapshot != null && mySnapshot.closed)
				throw new SnapshotClosedException();
		}

		/* ---------------- Utilities -------------- */

		boolean tooLow(Object key, Comparator<? super K> cmp) {
			int c;
			return (lo != null && ((c = cpr(cmp, key, lo)) < 0 || (c == 0 && !loInclusive)));
		}

		boolean tooHigh(Object key, Comparator<? super K> cmp) {

			int c;
			return (hi != null && ((c = cpr(cmp, key, hi)) > 0 || (c == 0 && !hiInclusive)));
		}

		boolean inBounds(Object key, Comparator<? super K> cmp) {
			return !tooLow(key, cmp) && !tooHigh(key, cmp);
		}

		void checkKeyBounds(K key, Comparator<? super K> cmp) {
			if (key == null)
				throw new NullPointerException();
			if (!inBounds(key, cmp))
				throw new IllegalArgumentException("key out of range");
		}

		/**
		 * Returns true if node key is less than upper bound of range.
		 */
		boolean isBeforeEnd(Quad<Node<K, V>, Revision<K, V>, Integer, Node<K, V>> t, Comparator<? super K> cmp) {
			if (t == null)
				return false;
			if (hi == null)
				return true;
			K k = t.second.getValue().getKeyByIndex(t.third);
			if (k == null) // pass by markers and headers
				return true;
			int c = cpr(cmp, k, hi);
			return c < 0 || (c == 0 && hiInclusive);
		}

		boolean debugIsBeforeEnd(Quad<Node<K, V>, Revision<K, V>, Integer, Object> t, Comparator<? super K> cmp) {
			if (t == null)
				return false;
			if (hi == null)
				return true;
			K k = t.second.getValue().getKeyByIndex(t.third);
			if (k == null) // pass by markers and headers
				return true;
			int c = cpr(cmp, k, hi);
			return c < 0 || (c == 0 && hiInclusive);
		}

		/**
		 * Returns lowest node. This node might not be in range, so most usages need to
		 * check bounds.
		 */
		Quad<Node<K, V>, Revision<K, V>, Integer, Node<K, V>> loNode(Comparator<? super K> cmp) {
			long ver = effectiveVersion();
			if (lo == null)
				return m.findFirst(ver);
			else if (loInclusive)
				return m.findNear(lo, GT | EQ, cmp, ver);
			else
				return m.findNear(lo, GT, cmp, ver);
		}

		/**
		 * Returns highest node. This node might not be in range, so most usages need to
		 * check bounds.
		 */
		Quad<Node<K, V>, Revision<K, V>, Integer, Node<K, V>> hiNode(Comparator<? super K> cmp) {
			long ver = effectiveVersion();
			if (hi == null)
				return m.findLast(ver);
			else if (hiInclusive)
				return m.findNear(hi, LT | EQ, cmp, ver);
			else
				return m.findNear(hi, LT, cmp, ver);
		}

		/**
		 * Returns lowest absolute key (ignoring directionality).
		 */
		K lowestKey() {
			Comparator<? super K> cmp = m.comparator;
			Quad<Node<K, V>, Revision<K, V>, Integer, Node<K, V>> t = loNode(cmp);
			if (isBeforeEnd(t, cmp))
				return t.second.getValue().getKeyByIndex(t.third);
			else
				throw new NoSuchElementException();
		}

		/**
		 * Returns highest absolute key (ignoring directionality).
		 */
		K highestKey() {
			Comparator<? super K> cmp = m.comparator;
			Quad<Node<K, V>, Revision<K, V>, Integer, Node<K, V>> t = hiNode(cmp);
			if (t != null) {
				K key = t.second.getValue().getKeyByIndex(t.third);
				if (inBounds(key, cmp))
					return key;
			}
			throw new NoSuchElementException();
		}

		Map.Entry<K, V> lowestEntry() {
			Comparator<? super K> cmp = m.comparator;
			for (;;) {
				Quad<Node<K, V>, Revision<K, V>, Integer, Node<K, V>> n;
//				V v;
				if ((n = loNode(cmp)) == null || !isBeforeEnd(n, cmp))
					return null;
				else
					return n.second.getValue().getByIndex(n.third);
			}
		}

		Map.Entry<K, V> highestEntry() {
			Comparator<? super K> cmp = m.comparator;
			for (;;) {
				Quad<Node<K, V>, Revision<K, V>, Integer, Node<K, V>> n;
				if ((n = hiNode(cmp)) == null || !inBounds(n.second.getValue().getKeyByIndex(n.third), cmp))
					return null;
				else
					return n.second.getValue().getByIndex(n.third);
			}
		}

		Map.Entry<K, V> removeLowest() {
			throw new UnsupportedOperationException();
//			Comparator<? super K> cmp = m.comparator;
//			for (;;) {
//				Node<K, V> n;
//				K k;
//				V v;
//				if ((n = loNode(cmp)) == null)
//					return null;
//				else if (!inBounds((k = n.key), cmp))
//					return null;
//				else if ((v = m.doRemove(k, null)) != null)
//					return new AbstractMap.SimpleImmutableEntry<K, V>(k, v);
//			}
		}

		Map.Entry<K, V> removeHighest() {
			throw new UnsupportedOperationException();
//			Comparator<? super K> cmp = m.comparator;
//			for (;;) {
//				Node<K, V> n;
//				K k;
//				V v;
//				if ((n = hiNode(cmp)) == null)
//					return null;
//				else if (!inBounds((k = n.key), cmp))
//					return null;
//				else if ((v = m.doRemove(k, null)) != null)
//					return new AbstractMap.SimpleImmutableEntry<K, V>(k, v);
//			}
		}

		/**
		 * Submap version of findNearEntry.
		 */
		Map.Entry<K, V> getNearEntry(K key, int rel) {
			Comparator<? super K> cmp = m.comparator;
			if (isDescending) { // adjust relation for direction
				if ((rel & LT) == 0)
					rel |= LT;
				else
					rel &= ~LT;
			}
			if (tooLow(key, cmp))
				return ((rel & LT) != 0) ? null : lowestEntry();
			if (tooHigh(key, cmp))
				return ((rel & LT) != 0) ? highestEntry() : null;
			Map.Entry<K, V> e = m.findNearEntry(key, rel, cmp, effectiveVersion());
			if (e == null || !inBounds(e.getKey(), cmp))
				return null;
			else
				return e;
		}

		// Almost the same as getNearEntry, except for keys
		K getNearKey(K key, int rel) {
			Comparator<? super K> cmp = m.comparator;
			if (isDescending) { // adjust relation for direction
				if ((rel & LT) == 0)
					rel |= LT;
				else
					rel &= ~LT;
			}
			if (tooLow(key, cmp)) {
				if ((rel & LT) == 0) {
					Quad<Node<K, V>, Revision<K, V>, Integer, Node<K, V>> n = loNode(cmp);
					if (isBeforeEnd(n, cmp))
						return n.second.getValue().getKeyByIndex(n.third);
				}
				return null;
			}
			if (tooHigh(key, cmp)) {
				if ((rel & LT) != 0) {
					Quad<Node<K, V>, Revision<K, V>, Integer, Node<K, V>> n = hiNode(cmp);
					if (n != null) {
						K last = n.second.getValue().getKeyByIndex(n.third);
						if (inBounds(last, cmp))
							return last;
					}
				}
				return null;
			}
			for (;;) {
				Quad<Node<K, V>, Revision<K, V>, Integer, Node<K, V>> n = m.findNear(key, rel, cmp, effectiveVersion());
				if (n == null)
					return null;
				K k = n.second.getValue().getKeyByIndex(n.third);
				if (!inBounds(k, cmp))
					return null;
				return k;
			}
		}

		/* ---------------- Map API methods -------------- */

		@Override
		public boolean containsKey(Object key) {
			if (key == null)
				throw new NullPointerException();
			checkSnapshot();
			return inBounds(key, m.comparator) && m.containsKey(key, effectiveVersion());
		}

		@Override
		public V get(Object key) {
			if (key == null)
				throw new NullPointerException();
			checkSnapshot();
			if (!inBounds(key, m.comparator))
				return null;

			return m.get(key, effectiveVersion());
		}

		@Override
		public V put(K key, V value) {
			throw new UnsupportedOperationException();
		}

		@Override
		public V remove(Object key) {
			throw new UnsupportedOperationException();
		}

		@Override
		public int size() {
			checkSnapshot();

			long count = 0;
			for (@SuppressWarnings("unused") Map.Entry<K, V> entry : entrySet()) {
				count++;
			}

			return count >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) count;
		}

		@Override
		public boolean isEmpty() {
			checkSnapshot();
			Comparator<? super K> cmp = m.comparator;
			return !isBeforeEnd(loNode(cmp), cmp);
		}

		@Override
		public boolean containsValue(Object value) {
			if (value == null)
				throw new NullPointerException();
			checkSnapshot();
			for (Map.Entry<K, V> entry : entrySet()) {
				if (entry.getValue().equals(value))
					return true;
			}
			return false;
		}

		@Override
		public void clear() {
			checkSnapshot();
			throw new UnsupportedOperationException();
//			Comparator<? super K> cmp = m.comparator;
//			for (Node<K, V> n = loNode(cmp); isBeforeEnd(n, cmp); n = n.next) {
//				if (n.val != null)
//					m.remove(n.key);
//			}
		}

		@Override
		public void forEach(BiConsumer<? super K, ? super V> action) {
			long[] statsArray = null;
			if (STATISTICS)
				statsArray = new long[9];

			Objects.requireNonNull(action);
			checkSnapshot();
			Comparator<? super K> cmp = m.comparator;
			VarHandle.acquireFence();

			Quad<Node<K, V>, Revision<K, V>, Integer, Node<K, V>> t = loNode(cmp);
			if (t == null) {
				if (STATISTICS)
					m.stats.get().updateSubMapForEach(statsArray);

				return;
			}

			var currentNode = t.first;
			var currentRevision = t.second;
			var currentMVal = t.second.getValue();
			var currentIndex = t.third;
			var nextNode = t.fourth;

			K refKey = null;

			outer: while (true) {
				if (STATISTICS)
					statsArray[0]++;

				boolean more;
				int toIndex;
				if (nextNode != null && !tooHigh(nextNode.key, m.comparator)) {
					if (STATISTICS)
						statsArray[1]++;

					toIndex = currentMVal.size();
					more = true;
				} else if (nextNode == null && currentMVal.size() != 0
						&& !tooHigh(currentMVal.lastKey(), m.comparator)) {
					if (STATISTICS)
						statsArray[2]++;

					toIndex = currentMVal.size();
					more = false;
				} else {
					if (STATISTICS)
						statsArray[3]++;

					int index = currentMVal.indexOfKeyInMultiVal(hi);
					if (index < 0)
						toIndex = -index - 1;
					else
						toIndex = hiInclusive ? index + 1 : index;
					more = false;
				}

				if (currentIndex < toIndex) {
					if (STATISTICS)
						statsArray[4]++;

					@SuppressWarnings("unchecked")
					var keys = (K[]) currentMVal.getKeys();
					@SuppressWarnings("unchecked")
					var values = (V[]) currentMVal.getValues();
					for (int i = currentIndex; i < toIndex; i++)
						action.accept(keys[i], values[i]);
					refKey = keys[toIndex - 1];
				}

				if (!more)
					break outer;

				while (true) {
					if (STATISTICS)
						statsArray[5]++;

					Revision<K, V> head = null;

					currentNode = nextNode;
					while (true) {
						if (currentNode == null)
							break outer;

						nextNode = currentNode.acquireNext();

						head = currentNode.acquireRevisionHead();

						if (currentNode.getType() == Node.TEMP_SPLIT) {
							if (STATISTICS)
								statsArray[6]++;

							currentNode = nextNode;
							continue;
						}

						if (head.getType() == Revision.MERGE_TERMINATOR) {
							if (STATISTICS)
								statsArray[7]++;

							m.helpMergeTerminator((MergeTerminatorRevision<K, V>) head);
							m.cleanTerminatedNode(currentNode.key);

							var quad = m.findNear(refKey, GT, comparator(), effectiveVersion());
							if (quad != null)
								currentNode = quad.first;
							else
								currentNode = null;
							continue;
						}

						break;
					}

					assert head != null;
					currentRevision = m.retrieveRevision(currentNode, null, effectiveVersion(), head);
					if (currentRevision != null) {
						if (STATISTICS)
							statsArray[8]++;

						currentMVal = currentRevision.getValue();
						if (cpr(m.comparator, refKey, currentRevision.getValue().firstKey()) < 0) {
							currentIndex = 0;
						} else {
							int index = currentMVal.indexOfKeyInMultiVal(refKey);
							if (index >= 0)
								currentIndex = index + 1;
							else
								currentIndex = -index - 1;
						}
						break;
					}
				}
			}

			if (STATISTICS)
				m.stats.get().updateSubMapForEach(statsArray);
		}

		/* ---------------- ConcurrentMap API methods -------------- */

		@Override
		public V putIfAbsent(K key, V value) {
			checkKeyBounds(key, m.comparator);
			checkSnapshot();
			return m.putIfAbsent(key, value);
		}

		@Override
		public boolean remove(Object key, Object value) {
			checkSnapshot();
			return inBounds(key, m.comparator) && m.remove(key, value);
		}

		@Override
		public boolean replace(K key, V oldValue, V newValue) {
			checkKeyBounds(key, m.comparator);
			checkSnapshot();
			return m.replace(key, oldValue, newValue);
		}

		@Override
		public V replace(K key, V value) {
			checkKeyBounds(key, m.comparator);
			checkSnapshot();
			return m.replace(key, value);
		}

		/* ---------------- SortedMap API methods -------------- */

		@Override
		public Comparator<? super K> comparator() {
			checkSnapshot();
			Comparator<? super K> cmp = m.comparator();
			if (isDescending)
				return Collections.reverseOrder(cmp);
			else
				return cmp;
		}

		/**
		 * Utility to create submaps, where given bounds override unbounded(null) ones
		 * and/or are checked against bounded ones.
		 */
		private SubMap<K, V> newSubMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
			Comparator<? super K> cmp = m.comparator;
			if (isDescending) { // flip senses
				K tk = fromKey;
				fromKey = toKey;
				toKey = tk;
				boolean ti = fromInclusive;
				fromInclusive = toInclusive;
				toInclusive = ti;
			}
			if (lo != null) {
				if (fromKey == null) {
					fromKey = lo;
					fromInclusive = loInclusive;
				} else {
					int c = cpr(cmp, fromKey, lo);
					if (c < 0 || (c == 0 && !loInclusive && fromInclusive))
						throw new IllegalArgumentException("key out of range");
				}
			}
			if (hi != null) {
				if (toKey == null) {
					toKey = hi;
					toInclusive = hiInclusive;
				} else {
					int c = cpr(cmp, toKey, hi);
					if (c > 0 || (c == 0 && !hiInclusive && toInclusive))
						throw new IllegalArgumentException("key out of range");
				}
			}
			return new SubMap<K, V>(m, fromKey, fromInclusive, toKey, toInclusive, isDescending, mySnapshot);
		}

		@Override
		public SubMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
			if (fromKey == null || toKey == null)
				throw new NullPointerException();
			checkSnapshot();
			return newSubMap(fromKey, fromInclusive, toKey, toInclusive);
		}

		@Override
		public SubMap<K, V> headMap(K toKey, boolean inclusive) {
			if (toKey == null)
				throw new NullPointerException();
			checkSnapshot();
			return newSubMap(null, false, toKey, inclusive);
		}

		@Override
		public SubMap<K, V> tailMap(K fromKey, boolean inclusive) {
			if (fromKey == null)
				throw new NullPointerException();
			checkSnapshot();
			return newSubMap(fromKey, inclusive, null, false);
		}

		@Override
		public SubMap<K, V> subMap(K fromKey, K toKey) {
			checkSnapshot();
			return subMap(fromKey, true, toKey, false);
		}

		@Override
		public SubMap<K, V> headMap(K toKey) {
			checkSnapshot();
			return headMap(toKey, false);
		}

		@Override
		public SubMap<K, V> tailMap(K fromKey) {
			checkSnapshot();
			return tailMap(fromKey, true);
		}

		@Override
		public SubMap<K, V> descendingMap() {
			checkSnapshot();
			return new SubMap<K, V>(m, lo, loInclusive, hi, hiInclusive, !isDescending, mySnapshot);
		}

		/* ---------------- Relational methods -------------- */

		@Override
		public Map.Entry<K, V> ceilingEntry(K key) {
			checkSnapshot();
			return getNearEntry(key, GT | EQ);
		}

		@Override
		public K ceilingKey(K key) {
			checkSnapshot();
			return getNearKey(key, GT | EQ);
		}

		@Override
		public Map.Entry<K, V> lowerEntry(K key) {
			checkSnapshot();
			return getNearEntry(key, LT);
		}

		@Override
		public K lowerKey(K key) {
			checkSnapshot();
			return getNearKey(key, LT);
		}

		@Override
		public Map.Entry<K, V> floorEntry(K key) {
			checkSnapshot();
			return getNearEntry(key, LT | EQ);
		}

		@Override
		public K floorKey(K key) {
			checkSnapshot();
			return getNearKey(key, LT | EQ);
		}

		@Override
		public Map.Entry<K, V> higherEntry(K key) {
			checkSnapshot();
			return getNearEntry(key, GT);
		}

		@Override
		public K higherKey(K key) {
			checkSnapshot();
			return getNearKey(key, GT);
		}

		@Override
		public K firstKey() {
			checkSnapshot();
			return isDescending ? highestKey() : lowestKey();
		}

		@Override
		public K lastKey() {
			checkSnapshot();
			return isDescending ? lowestKey() : highestKey();
		}

		@Override
		public Map.Entry<K, V> firstEntry() {
			checkSnapshot();
			return isDescending ? highestEntry() : lowestEntry();
		}

		@Override
		public Map.Entry<K, V> lastEntry() {
			checkSnapshot();
			return isDescending ? lowestEntry() : highestEntry();
		}

		@Override
		public Map.Entry<K, V> pollFirstEntry() {
			checkSnapshot();
			return isDescending ? removeHighest() : removeLowest();
		}

		@Override
		public Map.Entry<K, V> pollLastEntry() {
			checkSnapshot();
			return isDescending ? removeLowest() : removeHighest();
		}

		/* ---------------- Submap Views -------------- */

		@Override
		public NavigableSet<K> keySet() {
			checkSnapshot();
			return new KeySet<>(this);
		}

		@Override
		public NavigableSet<K> navigableKeySet() {
			checkSnapshot();
			return new KeySet<>(this);
		}

		@Override
		public Collection<V> values() {
			checkSnapshot();
			return new Values<>(this);
		}

		@Override
		public Set<Map.Entry<K, V>> entrySet() {
			checkSnapshot();
			return new EntrySet<K, V>(this);
		}

		@Override
		public NavigableSet<K> descendingKeySet() {
			checkSnapshot();
			return descendingMap().navigableKeySet();
		}

		@Override
		public String toString() {
			checkSnapshot();
			if (m instanceof Jiffy)
				return m.toString(effectiveVersion());
			return m.toString();
		}

		/**
		 * Variant of main Iter class to traverse through submaps. Also serves as
		 * back-up Spliterator for views.
		 */
		// TODO debug public
		public abstract class SubMapIter<T> implements Iterator<T>, Spliterator<T> {
			Node<K, V> currentNode;
			Node<K, V> nextNode; // only for ascendingIterator

			Revision<K, V> currentRevision;
			Iterator<Map.Entry<K, V>> currentIterator;
			K refKey; // maxKey for ascending iterator, minKey for descending iterator

			Map.Entry<K, V> nextEntry;

			public List<Node<K, V>> debugNodes = null;
			public List<Revision<K, V>> debugRevisions = null;
			public List<Integer> debugIndices = null;
			public List<K> debugLookedUpKeys = null;

			SubMapIter() {
				VarHandle.acquireFence();
				Comparator<? super K> cmp = m.comparator;

				Quad<Node<K, V>, Revision<K, V>, Integer, Node<K, V>> t;
				if (isDescending) {
					t = hiNode(cmp);
					if (t != null) {
						currentNode = t.first;
						currentRevision = t.second;
						currentIterator = t.second.getValue().descendingIterator(t.third);

						if (!rewindIteratorForwards())
							descend();
						if (nextEntry != null && tooLow(nextEntry.getKey(), m.comparator))
							nextEntry = null;
					}
				} else {
					t = loNode(cmp);
					if (t != null) {
						currentNode = t.first;
						currentRevision = t.second;
						currentIterator = t.second.getValue().iterator(t.third);

						nextNode = t.fourth;
						if (!rewindIteratorForwards())
							ascend();
						if (nextEntry != null && tooHigh(nextEntry.getKey(), m.comparator))
							nextEntry = null;
					}
				}
			}

			public Revision<K, V> debugGetCurrentRevision() {
				return currentRevision;
			}

			public String debugGetHistory() {
				StringBuilder builder = new StringBuilder();

				builder.append("Iterator history:\n");

				builder.append("- nodes:\n");
				if (debugNodes != null)
					for (var n : debugNodes) {
						builder.append("  * " + n + "\n");
					}

				builder.append("- revisions:\n");
				if (debugRevisions != null)
					for (var r : debugRevisions) {
						builder.append("  * " + r + "\n");
					}

				builder.append("- lookedUpKeys:\n");
				if (debugLookedUpKeys != null)
					for (var k : debugLookedUpKeys) {
						builder.append("  * " + k + "\n");
					}

				builder.append("- indices:\n");
				if (debugIndices != null)
					for (var i : debugIndices) {
						builder.append("  * " + i + "\n");
					}

				return builder.toString();
			}

			public final boolean hasNext() {
				return nextEntry != null;
			}

			final void advance() {
				if (nextEntry == null)
					throw new NoSuchElementException();
				if (isDescending)
					descend();
				else
					ascend();
			}

			boolean rewindIteratorForwards() {
				if (currentIterator == null)
					return false;

				if (currentIterator.hasNext()) {
					nextEntry = currentIterator.next();
					refKey = nextEntry.getKey();
					return true;
				}

				nextEntry = null;
				return false;
			}

			/** Advances next to higher entry. */
			final void ascend() {
				long[] statsArray = null;
				if (STATISTICS)
					statsArray = new long[5];

				outer: {
					if (currentIterator == null)
						break outer;

					while (true) {
						if (STATISTICS)
							statsArray[0]++;

						if (rewindIteratorForwards()) { // nextPair is set
							if (tooHigh(nextEntry.getKey(), m.comparator))
								nextEntry = null;
							break outer;
						}

						Revision<K, V> head = null;

						currentNode = nextNode;
						while (true) {
							if (STATISTICS)
								statsArray[1]++;

							if (currentNode == null) {
								currentIterator = null;
								nextEntry = null;

								break outer;
							}

							nextNode = currentNode.acquireNext();
							head = currentNode.acquireRevisionHead();

							if (currentNode.getType() == Node.TEMP_SPLIT) {
								if (STATISTICS)
									statsArray[2]++;

								currentNode = nextNode;
								continue;
							}

							if (head.getType() == Revision.MERGE_TERMINATOR) {
								if (STATISTICS)
									statsArray[3]++;

								m.helpMergeTerminator((MergeTerminatorRevision<K, V>) head);
								m.cleanTerminatedNode(currentNode.key);

								var quad = m.findNear(refKey, GT, comparator(), effectiveVersion());
								if (quad != null)
									currentNode = quad.first;
								else
									currentNode = null;
								continue;
							}

							break;
						}

						assert head != null;
						currentRevision = m.retrieveRevision(currentNode, null, effectiveVersion(), head);
						if (currentRevision != null) {
							if (STATISTICS)
								statsArray[4]++;
							if (refKey == null) {
								currentIterator = currentRevision.getValue().iterator();
							} else if (cpr(m.comparator, refKey, currentRevision.getValue().lastKey()) < 0) {
								int index = currentRevision.getValue().indexOfKeyInMultiVal(refKey, GT);
								currentIterator = currentRevision.getValue().iterator(index);
							}
						}
					}
				}

				if (STATISTICS)
					m.stats.get().updateSubMapIterAscend(statsArray);
			}

			private void descend() {
				long[] statsArray = null;
				if (STATISTICS)
					statsArray = new long[2];

				outer: {
					if (currentIterator == null)
						break outer;

					while (true) {
						if (STATISTICS)
							statsArray[0]++;

						if (rewindIteratorForwards()) { // nextPair is set
							if (tooLow(nextEntry.getKey(), m.comparator))
								nextEntry = null;
							break outer;
						}

						if (currentNode.key == null) {
							currentIterator = null;
							nextEntry = null;
							break outer;
						}

						if (STATISTICS)
							statsArray[1]++;

						K keyToSearch = currentRevision.getValue().firstKey();
						var t = m.findNear(keyToSearch, LT, m.comparator, effectiveVersion());

						if (t == null) {
							currentIterator = null;
							nextEntry = null;
							break outer;
						}

						currentNode = t.first;
						currentRevision = t.second;
						currentIterator = currentRevision.getValue().descendingIterator(t.third);
					}
				}

				if (STATISTICS)
					m.stats.get().updateSubMapIterDescend(statsArray);
			}

			@Override
			public void remove() {
				checkSnapshot();
				throw new UnsupportedOperationException();
//				Node<K, V> l = lastReturned;
//				if (l == null)
//					throw new IllegalStateException();
//				m.remove(l.key);
//				lastReturned = null;
			}

			@Override
			public Spliterator<T> trySplit() {
				checkSnapshot();
				return null;
			}

			@Override
			public boolean tryAdvance(Consumer<? super T> action) {
				checkSnapshot();
				if (hasNext()) {
					action.accept(next());
					return true;
				}
				return false;
			}

			@Override
			public void forEachRemaining(Consumer<? super T> action) {
				checkSnapshot();
				while (hasNext())
					action.accept(next());
			}

			@Override
			public long estimateSize() {
				checkSnapshot();
				return Long.MAX_VALUE;
			}

		}

		final class SubMapValueIterator extends SubMapIter<V> {
			@Override
			public V next() {
				checkSnapshot();
				V v = nextEntry == null ? null : nextEntry.getValue();
				advance();
				return v;
			}

			@Override
			public int characteristics() {
				checkSnapshot();
				return 0;
			}
		}

		final class SubMapKeyIterator extends SubMapIter<K> {
			@Override
			public K next() {
				checkSnapshot();
				K k = nextEntry == null ? null : nextEntry.getKey();
				advance();
				return k;
			}

			@Override
			public int characteristics() {
				checkSnapshot();
				return Spliterator.DISTINCT | Spliterator.ORDERED | Spliterator.SORTED;
			}

			@Override
			public final Comparator<? super K> getComparator() {
				checkSnapshot();
				return SubMap.this.comparator();
			}
		}

		// TODO public debug
		public final class SubMapEntryIterator extends SubMapIter<Map.Entry<K, V>> {
			@Override
			public Map.Entry<K, V> next() {
				checkSnapshot();
				var entry = nextEntry;
				advance();
				return entry;
			}

			public Pair<Map.Entry<K, V>, Revision<K, V>> nextDebug() {
				checkSnapshot();
				var p = nextEntry;
				var r = currentRevision;
				advance();
				return new Pair<>(p, r);
			}

			@Override
			public int characteristics() {
				checkSnapshot();
				return Spliterator.DISTINCT;
			}
		}

		@Override
		public void putAll(Map<? extends K, ? extends V> m) {
			checkSnapshot();
			throw new UnsupportedOperationException();
		}

		/* ---------------- Snapshots -------------- */

		@Override
		public long getSnapshotVersion() {
			checkSnapshot();
			return acquireVersion();
		}

		@Override
		public long getCurrentVersion() {
			checkSnapshot();
			return m.getCurrentVersion();
		}

		@Override
		public MultiversionNavigableMapSnapshot<K, V> snapshot(long version) {
			checkSnapshot();
			var subMap = new SubMap<K, V>(m, lo, loInclusive, hi, hiInclusive, isDescending, version);
			m.register(subMap, false);
			return subMap;
		}

		@Override
		public MultiversionNavigableMapSnapshot<K, V> snapshot() {
			checkSnapshot();
			var subMap = new SubMap<K, V>(m, lo, loInclusive, hi, hiInclusive, isDescending, getCurrentVersion());
			m.register(subMap, true);
			return subMap;
		}

		@Override
		public void close() {
			checkSnapshot();
			if (mySnapshot == this) {
				m.unregister(this);
				closed = true;
			} else
				throw new UnsupportedOperationException();
		}

		@Override
		public void update() {
			checkSnapshot();
			if (mySnapshot == this)
				m.updateSnapshot(this);
			else
				throw new UnsupportedOperationException();
		}

		@Override
		public void put(Batch<K, V> batch) {
			throw new UnsupportedOperationException();
		}
	}

	
//	// TODO default Map method overrides
//
//    public void forEach(BiConsumer<? super K, ? super V> action) {
//        if (action == null) throw new NullPointerException();
//        Node<K,V> b, n; V v;
//        if ((b = baseHead()) != null) {
//            while ((n = b.next) != null) {
//                if ((v = n.val) != null)
//                    action.accept(n.key, v);
//                b = n;
//            }
//        }
//    }
//
//    public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
//        if (function == null) throw new NullPointerException();
//        Node<K,V> b, n; V v;
//        if ((b = baseHead()) != null) {
//            while ((n = b.next) != null) {
//                while ((v = n.val) != null) {
//                    V r = function.apply(n.key, v);
//                    if (r == null) throw new NullPointerException();
//                    if (VAL.compareAndSet(n, v, r))
//                        break;
//                }
//                b = n;
//            }
//        }
//    }
//
//    /**
//     * Helper method for EntrySet.removeIf.
//     */
//    boolean removeEntryIf(Predicate<? super Entry<K,V>> function) {
//        if (function == null) throw new NullPointerException();
//        boolean removed = false;
//        Node<K,V> b, n; V v;
//        if ((b = baseHead()) != null) {
//            while ((n = b.next) != null) {
//                if ((v = n.val) != null) {
//                    K k = n.key;
//                    Map.Entry<K,V> e = new AbstractMap.SimpleImmutableEntry<>(k, v);
//                    if (function.test(e) && remove(k, v))
//                        removed = true;
//                }
//                b = n;
//            }
//        }
//        return removed;
//    }
//
//    /**
//     * Helper method for Values.removeIf.
//     */
//    boolean removeValueIf(Predicate<? super V> function) {
//        if (function == null) throw new NullPointerException();
//        boolean removed = false;
//        Node<K,V> b, n; V v;
//        if ((b = baseHead()) != null) {
//            while ((n = b.next) != null) {
//                if ((v = n.val) != null && function.test(v) && remove(n.key, v))
//                    removed = true;
//                b = n;
//            }
//        }
//        return removed;
//    }
//
//    /**
//     * Base class providing common structure for Spliterators.
//     * (Although not all that much common functionality; as usual for
//     * view classes, details annoyingly vary in key, value, and entry
//     * subclasses in ways that are not worth abstracting out for
//     * internal classes.)
//     *
//     * The basic split strategy is to recursively descend from top
//     * level, row by row, descending to next row when either split
//     * off, or the end of row is encountered. Control of the number of
//     * splits relies on some statistical estimation: The expected
//     * remaining number of elements of a skip list when advancing
//     * either across or down decreases by about 25%.
//     */
//    abstract static class CSLMSpliterator<K,V> {
//        final Comparator<? super K> comparator;
//        final K fence;     // exclusive upper bound for keys, or null if to end
//        Index<K,V> row;    // the level to split out
//        Node<K,V> current; // current traversal node; initialize at origin
//        long est;          // size estimate
//        CSLMSpliterator(Comparator<? super K> comparator, Index<K,V> row,
//                        Node<K,V> origin, K fence, long est) {
//            this.comparator = comparator; this.row = row;
//            this.current = origin; this.fence = fence; this.est = est;
//        }
//
//        public final long estimateSize() { return est; }
//    }
//
//    static final class KeySpliterator<K,V> extends CSLMSpliterator<K,V>
//        implements Spliterator<K> {
//        KeySpliterator(Comparator<? super K> comparator, Index<K,V> row,
//                       Node<K,V> origin, K fence, long est) {
//            super(comparator, row, origin, fence, est);
//        }
//
//        public KeySpliterator<K,V> trySplit() {
//            Node<K,V> e; K ek;
//            Comparator<? super K> cmp = comparator;
//            K f = fence;
//            if ((e = current) != null && (ek = e.key) != null) {
//                for (Index<K,V> q = row; q != null; q = row = q.down) {
//                    Index<K,V> s; Node<K,V> b, n; K sk;
//                    if ((s = q.right) != null && (b = s.node) != null &&
//                        (n = b.next) != null && n.val != null &&
//                        (sk = n.key) != null && cpr(cmp, sk, ek) > 0 &&
//                        (f == null || cpr(cmp, sk, f) < 0)) {
//                        current = n;
//                        Index<K,V> r = q.down;
//                        row = (s.right != null) ? s : s.down;
//                        est -= est >>> 2;
//                        return new KeySpliterator<K,V>(cmp, r, e, sk, est);
//                    }
//                }
//            }
//            return null;
//        }
//
//        public void forEachRemaining(Consumer<? super K> action) {
//            if (action == null) throw new NullPointerException();
//            Comparator<? super K> cmp = comparator;
//            K f = fence;
//            Node<K,V> e = current;
//            current = null;
//            for (; e != null; e = e.next) {
//                K k;
//                if ((k = e.key) != null && f != null && cpr(cmp, f, k) <= 0)
//                    break;
//                if (e.val != null)
//                    action.accept(k);
//            }
//        }
//
//        public boolean tryAdvance(Consumer<? super K> action) {
//            if (action == null) throw new NullPointerException();
//            Comparator<? super K> cmp = comparator;
//            K f = fence;
//            Node<K,V> e = current;
//            for (; e != null; e = e.next) {
//                K k;
//                if ((k = e.key) != null && f != null && cpr(cmp, f, k) <= 0) {
//                    e = null;
//                    break;
//                }
//                if (e.val != null) {
//                    current = e.next;
//                    action.accept(k);
//                    return true;
//                }
//            }
//            current = e;
//            return false;
//        }
//
//        public int characteristics() {
//            return Spliterator.DISTINCT | Spliterator.SORTED |
//                Spliterator.ORDERED | Spliterator.CONCURRENT |
//                Spliterator.NONNULL;
//        }
//
//        public final Comparator<? super K> getComparator() {
//            return comparator;
//        }
//    }
//    // factory method for KeySpliterator
//    final KeySpliterator<K,V> keySpliterator() {
//        Index<K,V> h; Node<K,V> n; long est;
//        VarHandle.acquireFence();
//        if ((h = head) == null) {
//            n = null;
//            est = 0L;
//        }
//        else {
//            n = h.node;
//            est = getAdderCount();
//        }
//        return new KeySpliterator<K,V>(comparator, h, n, null, est);
//    }
//
//    static final class ValueSpliterator<K,V> extends CSLMSpliterator<K,V>
//        implements Spliterator<V> {
//        ValueSpliterator(Comparator<? super K> comparator, Index<K,V> row,
//                       Node<K,V> origin, K fence, long est) {
//            super(comparator, row, origin, fence, est);
//        }
//
//        public ValueSpliterator<K,V> trySplit() {
//            Node<K,V> e; K ek;
//            Comparator<? super K> cmp = comparator;
//            K f = fence;
//            if ((e = current) != null && (ek = e.key) != null) {
//                for (Index<K,V> q = row; q != null; q = row = q.down) {
//                    Index<K,V> s; Node<K,V> b, n; K sk;
//                    if ((s = q.right) != null && (b = s.node) != null &&
//                        (n = b.next) != null && n.val != null &&
//                        (sk = n.key) != null && cpr(cmp, sk, ek) > 0 &&
//                        (f == null || cpr(cmp, sk, f) < 0)) {
//                        current = n;
//                        Index<K,V> r = q.down;
//                        row = (s.right != null) ? s : s.down;
//                        est -= est >>> 2;
//                        return new ValueSpliterator<K,V>(cmp, r, e, sk, est);
//                    }
//                }
//            }
//            return null;
//        }
//
//        public void forEachRemaining(Consumer<? super V> action) {
//            if (action == null) throw new NullPointerException();
//            Comparator<? super K> cmp = comparator;
//            K f = fence;
//            Node<K,V> e = current;
//            current = null;
//            for (; e != null; e = e.next) {
//                K k; V v;
//                if ((k = e.key) != null && f != null && cpr(cmp, f, k) <= 0)
//                    break;
//                if ((v = e.val) != null)
//                    action.accept(v);
//            }
//        }
//
//        public boolean tryAdvance(Consumer<? super V> action) {
//            if (action == null) throw new NullPointerException();
//            Comparator<? super K> cmp = comparator;
//            K f = fence;
//            Node<K,V> e = current;
//            for (; e != null; e = e.next) {
//                K k; V v;
//                if ((k = e.key) != null && f != null && cpr(cmp, f, k) <= 0) {
//                    e = null;
//                    break;
//                }
//                if ((v = e.val) != null) {
//                    current = e.next;
//                    action.accept(v);
//                    return true;
//                }
//            }
//            current = e;
//            return false;
//        }
//
//        public int characteristics() {
//            return Spliterator.CONCURRENT | Spliterator.ORDERED |
//                Spliterator.NONNULL;
//        }
//    }
//
//    // Almost the same as keySpliterator()
//    final ValueSpliterator<K,V> valueSpliterator() {
//        Index<K,V> h; Node<K,V> n; long est;
//        VarHandle.acquireFence();
//        if ((h = head) == null) {
//            n = null;
//            est = 0L;
//        }
//        else {
//            n = h.node;
//            est = getAdderCount();
//        }
//        return new ValueSpliterator<K,V>(comparator, h, n, null, est);
//    }
//
//    static final class EntrySpliterator<K,V> extends CSLMSpliterator<K,V>
//        implements Spliterator<Map.Entry<K,V>> {
//        EntrySpliterator(Comparator<? super K> comparator, Index<K,V> row,
//                         Node<K,V> origin, K fence, long est) {
//            super(comparator, row, origin, fence, est);
//        }
//
//        public EntrySpliterator<K,V> trySplit() {
//            Node<K,V> e; K ek;
//            Comparator<? super K> cmp = comparator;
//            K f = fence;
//            if ((e = current) != null && (ek = e.key) != null) {
//                for (Index<K,V> q = row; q != null; q = row = q.down) {
//                    Index<K,V> s; Node<K,V> b, n; K sk;
//                    if ((s = q.right) != null && (b = s.node) != null &&
//                        (n = b.next) != null && n.val != null &&
//                        (sk = n.key) != null && cpr(cmp, sk, ek) > 0 &&
//                        (f == null || cpr(cmp, sk, f) < 0)) {
//                        current = n;
//                        Index<K,V> r = q.down;
//                        row = (s.right != null) ? s : s.down;
//                        est -= est >>> 2;
//                        return new EntrySpliterator<K,V>(cmp, r, e, sk, est);
//                    }
//                }
//            }
//            return null;
//        }
//
//        public void forEachRemaining(Consumer<? super Map.Entry<K,V>> action) {
//            if (action == null) throw new NullPointerException();
//            Comparator<? super K> cmp = comparator;
//            K f = fence;
//            Node<K,V> e = current;
//            current = null;
//            for (; e != null; e = e.next) {
//                K k; V v;
//                if ((k = e.key) != null && f != null && cpr(cmp, f, k) <= 0)
//                    break;
//                if ((v = e.val) != null) {
//                    action.accept
//                        (new AbstractMap.SimpleImmutableEntry<K,V>(k, v));
//                }
//            }
//        }
//
//        public boolean tryAdvance(Consumer<? super Map.Entry<K,V>> action) {
//            if (action == null) throw new NullPointerException();
//            Comparator<? super K> cmp = comparator;
//            K f = fence;
//            Node<K,V> e = current;
//            for (; e != null; e = e.next) {
//                K k; V v;
//                if ((k = e.key) != null && f != null && cpr(cmp, f, k) <= 0) {
//                    e = null;
//                    break;
//                }
//                if ((v = e.val) != null) {
//                    current = e.next;
//                    action.accept
//                        (new AbstractMap.SimpleImmutableEntry<K,V>(k, v));
//                    return true;
//                }
//            }
//            current = e;
//            return false;
//        }
//
//        public int characteristics() {
//            return Spliterator.DISTINCT | Spliterator.SORTED |
//                Spliterator.ORDERED | Spliterator.CONCURRENT |
//                Spliterator.NONNULL;
//        }
//
//        public final Comparator<Map.Entry<K,V>> getComparator() {
//            // Adapt or create a key-based comparator
//            if (comparator != null) {
//                return Map.Entry.comparingByKey(comparator);
//            }
//            else {
//                return (Comparator<Map.Entry<K,V>> & Serializable) (e1, e2) -> {
//                    @SuppressWarnings("unchecked")
//                    Comparable<? super K> k1 = (Comparable<? super K>) e1.getKey();
//                    return k1.compareTo(e2.getKey());
//                };
//            }
//        }
//    }
//
//    // Almost the same as keySpliterator()
//    final EntrySpliterator<K,V> entrySpliterator() {
//        Index<K,V> h; Node<K,V> n; long est;
//        VarHandle.acquireFence();
//        if ((h = head) == null) {
//            n = null;
//            est = 0L;
//        }
//        else {
//            n = h.node;
//            est = getAdderCount();
//        }
//        return new EntrySpliterator<K,V>(comparator, h, n, null, est);
//    } 

	// VarHandle mechanics
	private static final VarHandle HEAD;
	private static final VarHandle NEXT;
	private static final VarHandle RIGHT;
	private static final VarHandle CURRENT_VERSION;
	private static final VarHandle SNAPSHOTS;
	static {
		try {
			MethodHandles.Lookup l = MethodHandles.lookup();
			HEAD = l.findVarHandle(Jiffy.class, "head", Index.class);
			NEXT = l.findVarHandle(Node.class, "next", Node.class);
			RIGHT = l.findVarHandle(Index.class, "right", Index.class);
			CURRENT_VERSION = l.findVarHandle(Jiffy.class, "currentVersion", long.class);
			SNAPSHOTS = l.findVarHandle(Jiffy.class, "snapshots", SubMap.class);
		} catch (ReflectiveOperationException e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	/* ---------------- Snapshots -------------- */

	@Override
	public long getSnapshotVersion() {
		return NEWEST_VERSION;
	}

	@Override
	public long getCurrentVersion() {
		if (USE_TSC) {
			return System.nanoTime() - startTime;
		} else {
			return (long) CURRENT_VERSION.getAcquire(this);
		}
	}

	@Override
	public MultiversionNavigableMapSnapshot<K, V> snapshot(long version) {
		var subMap = new SubMap<K, V>(this, null, false, null, false, false, version);
		register(subMap, false);
		return subMap;
	}

	@Override
	public MultiversionNavigableMapSnapshot<K, V> snapshot() {
		var subMap = new SubMap<K, V>(this, null, false, null, false, false, getCurrentVersion());
		register(subMap, true);
		return subMap;
	}

	/* ---------------- GC -------------- */

	void register(SubMap<K, V> snapshot, boolean update) {
		while (true) {
			var expected = (SubMap<K, V>) SNAPSHOTS.getAcquire(this);
			snapshot.nextSnapshot = expected;
			if (SNAPSHOTS.compareAndSet(this, expected, snapshot))
				break;
		}
		if (update)
			updateSnapshot(snapshot);
	}

	private void updateSnapshot(SubMap<K, V> snapshot) {
		long myVersion = getCurrentVersion();
		snapshot.setVersion(myVersion);
	}

	@SuppressWarnings("resource")
	void unregister(SubMap<K, V> snapshot) {
		snapshot.setVersion(Long.MAX_VALUE); // deactivate the snapshot

		while (true) {
			SubMap<K, V> head = (SubMap<K, V>) SNAPSHOTS.getAcquire(this);
			SubMap<K, V> soundPrev = null; // the last active snapshot before ours
			SubMap<K, V> soundNext = head; // soundNext == soundPrev.next or soundNext == head if soundPrev == null
			SubMap<K, V> current = head;
			while (current != null && current != snapshot) {
				var next = current.acquireNextSnapshot();
				if (current.acquireVersion() != Long.MAX_VALUE) {
					soundPrev = current;
					soundNext = next;
				}
				current = next;
			}

			if (current == null) // snapshot not reachable from the head => all done
				return;

			SubMap<K, V> next = current.acquireNextSnapshot();
			while (true) {
				while (next != null && next.acquireVersion() == Long.MAX_VALUE) { // find the first active snapshot
																					// after ours
					next = next.acquireNextSnapshot();
				}

				boolean success;
				if (soundPrev != null) {
					success = soundPrev.casNextSnapshot(soundNext, next);
				} else {
					success = SNAPSHOTS.compareAndSet(this, head, next);
				}

				if (success) {
					if (next != null && next.acquireVersion() == Long.MAX_VALUE) {
						next = next.acquireNextSnapshot();
						continue; // we will try to CAS one more time to remove even more deactivated nodes
					}
				}
				// no matter whether CAS was successful, or not, we have to check if snapshot is
				// reachable from the head
				break;
			}
		}
	}

	@SuppressWarnings("resource")
	private long computeMinSnapshotVersion(long upperBound) {
		long curMin = upperBound;
		SubMap<K, V> current = (SubMap<K, V>) SNAPSHOTS.getAcquire(this);
		while (current != null) {
			curMin = Long.min(curMin, current.acquireVersion());
			current = current.acquireNextSnapshot();
		}
		return curMin;
	}

	long getGcNum() {
		return computeMinSnapshotVersion(getCurrentVersion());
	}

	private long getGcNum(long upperBound) {
		return computeMinSnapshotVersion(upperBound);
	}

}