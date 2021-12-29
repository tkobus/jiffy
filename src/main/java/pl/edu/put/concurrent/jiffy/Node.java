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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

// Is public for debug in Conctest. Should be nothing

public class Node<K, V> {
	final static int REGULAR = 0;
	final static int TEMP_SPLIT = 1;
	final static int MARKER = 2;

	public final K key;
	Revision<K, V> revisionHead = null;
	Node<K, V> next;
	final Comparator<? super K> comparator;
	boolean readyToUnlink = false;

	protected int type;

	// Just for debug
	public Node<K, V> debugNode = null;
	public MergeRevision<K, V> debugMergeRev = null;

	Node(K key, Revision<K, V> revision, Node<K, V> next, Comparator<? super K> comparator) {
		this.type = REGULAR;
		this.key = key;
		this.revisionHead = revision;
		this.next = next;
		this.comparator = comparator;
	}

	// Previously delete marker, now used only for debug
	Node(Node<K, V> next) {
		this.type = MARKER;
		this.key = null;
		this.revisionHead = null;
		this.next = next;
		this.comparator = null;
	}

	public int getType() {
		return type;
	}

	@Override
	public String toString() {
		var rhcopy = revisionHead;
		return super.toString() + ", key: " + key + ", revHead: " + rhcopy
				+ (rhcopy instanceof SplitRevision<?, ?>
						? (" sib: " + ((SplitRevision<K, V>) rhcopy).sibling + " next: " + rhcopy.next)
						: "")
				+ " " + (rhcopy instanceof MergeTerminatorRevision<?, ?> ? rhcopy.listVersion() : "");
	}

	public String toString(boolean printRev, boolean printDebugNode) {
		if (printRev)
			return toString();
		return super.toString() + ", key: " + key + (printDebugNode ? debugNodeInfo() : "");
	}

	private String debugNodeInfo() {
		if (debugNode == null)
			return "";
		return " (merged into " + debugNode.toString(false, false) + " in version " + debugMergeRev.effectiveVersion()
				+ ")";
	}

	boolean isTerminated() {
		return (boolean) READY_TO_UNLINK.getOpaque(this);
	}

	protected Node<K, V> acquireNext() {
		return (Node<K, V>) NEXT.getAcquire(this);
	}

	Revision<K, V> acquireRevisionHead() {
		return (Revision<K, V>) REVISION_HEAD.getAcquire(this);
	}

	boolean terminate() {
		READY_TO_UNLINK.setOpaque(this, true);
		return true;
	}

	private Revision<K, V> returnStableRevision(K key, Revision<K, V> revision, long revisionVersion, long version) {
		Revision<K, V> ret = null;

		assert revisionVersion > 0;

		if (revisionVersion <= version) {
			if (revision.getType() == Revision.SPLIT) {
				SplitRevision<K, V> splitRevision = (SplitRevision<K, V>) revision;
				if (key != null && splitRevision.left
						&& Jiffy.cpr(comparator, key, splitRevision.getValue().lastKey()) > 0)
					ret = splitRevision.sibling;
				else
					ret = splitRevision;
			} else
				ret = revision;
		} else if (key == null && revision.getType() == Revision.MERGE) {
			ret = retrieveBulkRevision((MergeRevision<K, V>) revision, version);
		}

		return ret;
	}

	private boolean checkStableRevision(K key, Revision<K, V> revision, long revisionVersion, long version) {
		return revisionVersion <= version || (key == null && revision.getType() == Revision.MERGE);
	}

	protected Revision<K, V> debugGetRevision(K key, long version, Revision<K, V> head, Jiffy<K, V> map, Node<K, V> b,
			Node<K, V> n, List<Revision<K, V>> revisions) {
		Revision<K, V> revision;
		if (head != null)
			revision = head;
		else
			revision = acquireRevisionHead();

		Revision<K, V> ret = null;

		// For single puts, the optimistic version == currentVersion at the time of
		// creation of the revision.
		// if optimistic version is > version, its ok, we'll not use this version anyway
		// if descriptor == null, then its a single put, so its version will be >
		// currentVersion (and for sure version <= currentVersion)

		while (revision != null) {
			revisions.add(revision);
			// long revisionVersion = revision.getVersion();
			long revisionVersion = revision.effectiveVersion();

			if (revisionVersion > 0 && (ret = returnStableRevision(key, revision, revisionVersion, version)) != null)
				break;

			if (revisionVersion < 0) {
				revisionVersion *= -1; // optimistic version, so the final one will be >= revisionVersion
				if (revisionVersion <= version) {
					revisionVersion = map.helpPut(revision);
					// revisionVersion can be bogus if we helped a MergeTerminator, so
					// we need to find the proper merge revision and see if it's any good.
					// If the mergeRevision's version is good for us, we will find it, otherwise
					// we will get null?
					// if (revision instanceof HeavyMergeTerminatorRevision<?, ?>) {
					revisionVersion = revision.effectiveVersion();
					if (revision.getType() == Revision.MERGE_TERMINATOR) {
						MergeRevision<K, V> mergeRevision = map
								.findMergeRevisionSingle((MergeTerminatorRevision<K, V>) revision, revision.version * -1);

						if (mergeRevision != null && (ret = returnStableRevision(key, mergeRevision,
								mergeRevision.effectiveVersion(), version)) != null)
							break;
					} else {
						if ((ret = returnStableRevision(key, revision, revisionVersion, version)) != null)
							break;
					}
				}
			}

			if (revision.getType() == Revision.MERGE) {
				MergeRevision<K, V> mergeRevision = (MergeRevision<K, V>) revision;
				if (key == null) {
					ret = retrieveBulkRevision(mergeRevision, version);
					break;
				} else {
					if (Jiffy.cpr(comparator, key, mergeRevision.keyOfRightNode) < 0)
						revision = mergeRevision.acquireNext();
					else
						revision = mergeRevision.acquireRightNext();
				}
			} else {
				revision = revision.acquireNext();
			}
		}

//		DEBUG
//		if (ret != null) {
//			revisions.add(null);
//			revisions.add(null);
//			var xn = ret.next;
//			revisions.add(xn);
//			if (xn != null)
//				revisions.add(xn.next);
//			revisions.add(null);
//			revisions.add(null);
//		}

		return ret;
	}

	protected Revision<K, V> getRevision(K key, long version, Revision<K, V> head, Jiffy<K, V> map, long[] statsArray) {
		Revision<K, V> revision;
		if (head != null)
			revision = head;
		else
			revision = acquireRevisionHead();

		Revision<K, V> ret = null;

		// For single puts, the optimistic version == currentVersion at the time of
		// creation of the revision.
		// if optimistic version is > version, its ok, we'll not use this version anyway
		// if descriptor == null, then its a single put, so its version will be >
		// currentVersion (and for sure version <= currentVersion)

		while (revision != null) {
			if (Jiffy.STATISTICS)
				statsArray[0]++;

			long revisionVersion = revision.effectiveVersion();

			if (revisionVersion > 0 && checkStableRevision(key, revision, revisionVersion, version)) {
				ret = returnStableRevision(key, revision, revisionVersion, version);
				break;
			}

			if (revisionVersion < 0) {
				revisionVersion *= -1; // optimistic version, so the final one will be >= revisionVersion
				if (revisionVersion <= version) {
					if (Jiffy.STATISTICS)
						statsArray[1]++;

					revisionVersion = map.helpPut(revision);
					// revisionVersion can be bogus if we helped a MergeTerminator, so
					// we need to find the proper merge revision and see if it's any good.
					// If the mergeRevision's version is good for us, we will find it, otherwise
					// we will get null?
					// if (revision instanceof HeavyMergeTerminatorRevision<?, ?>) {
					revisionVersion = revision.effectiveVersion();
					if (revision.getType() == Revision.MERGE_TERMINATOR) {
						MergeRevision<K, V> mergeRevision = map
								.findMergeRevisionSingle((MergeTerminatorRevision<K, V>) revision, revision.version * -1);

						if (mergeRevision != null && (ret = returnStableRevision(key, mergeRevision,
								mergeRevision.effectiveVersion(), version)) != null)
							break;
					} else {
						if ((ret = returnStableRevision(key, revision, revisionVersion, version)) != null)
							break;
					}
				}
			}

			if (revision.getType() == Revision.MERGE) {
				MergeRevision<K, V> mergeRevision = (MergeRevision<K, V>) revision;
				if (key == null) {
					if (Jiffy.STATISTICS)
						statsArray[2]++;

					return retrieveBulkRevision(mergeRevision, version);
				} else {
					if (Jiffy.cpr(comparator, key, mergeRevision.keyOfRightNode) < 0)
						revision = mergeRevision.acquireNext();
					else
						revision = mergeRevision.acquireRightNext();
				}
			} else {
				revision = revision.acquireNext();
			}
		}

		return ret;
	}

	private Revision<K, V> retrieveBulkRevision(MergeRevision<K, V> mergeRevision, long version) {
		// we can assume that all versions in next and rightNext are committed
		List<Revision<K, V>> allRevisions = new ArrayList<>();
		Set<Revision<K, V>> mergeRevisions = new HashSet<>();
		findNested(mergeRevision.next, version, allRevisions, mergeRevisions);
		findNested(mergeRevision.rightNext, version, allRevisions, mergeRevisions);
		if (allRevisions.size() == 0)
			return null;

		MultiVal<K, V> mval = BulkMultiVal.createBulkRevision(comparator, allRevisions);
		Revision<K, V> revision = new Revision<>(mval, version, null);
		revision.bulk = true;
		revision.bulkRevs = allRevisions;
		return revision;
	}

	private void findNested(Revision<K, V> revision, long version, List<Revision<K, V>> allRevisions,
			Set<Revision<K, V>> mergeRevisions) {
		Revision<K, V> currentRevision = revision;
		while (true) {
			if (currentRevision == null)
				return;
			if (currentRevision.getType() == Revision.MERGE) {
				if (mergeRevisions.contains(currentRevision))
					return;
				mergeRevisions.add(currentRevision);
			}
			// if (currentRevision.version <= version) {
			if (currentRevision.effectiveVersion() <= version) {
				int listSize = allRevisions.size();
				if (currentRevision.getValue().size() != 0
						&& (listSize == 0 || !allRevisions.contains(currentRevision)))
					allRevisions.add(currentRevision);
				return;
			}
			if (currentRevision.getType() == Revision.MERGE) {
				MergeRevision<K, V> castRevision = (MergeRevision<K, V>) currentRevision;
				findNested(castRevision.next, version, allRevisions, mergeRevisions);
				findNested(castRevision.rightNext, version, allRevisions, mergeRevisions);
			} else {
				currentRevision = currentRevision.next;
			}
		}
	}

	protected Revision<K, V> getNewestRevision(K key, Revision<K, V> head, Jiffy<K, V> map, long[] statsArray) {
		Revision<K, V> revision = acquireRevisionHead();
		long revisionVersion = 0;

		while (revision != null) {
			if (Jiffy.STATISTICS)
				statsArray[0]++;

			if (revision.getType() == Revision.MERGE_TERMINATOR) {
				revision = revision.acquireNext();
				continue;
			}

			revisionVersion = revision.effectiveVersion();
			if (revisionVersion > 0)
				break;

			if (revision.getType() == Revision.MERGE) {
				MergeRevision<K, V> mergeRevision = (MergeRevision<K, V>) revision;
				if (key != null && Jiffy.cpr(comparator, key, mergeRevision.keyOfRightNode) >= 0) {
					revision = mergeRevision.acquireRightNext();
					continue;
				}
			}
			revision = revision.acquireNext();
		}

		if (revision != null)
			map.publishVersion(revisionVersion);

		if (revision != null && revision.getType() == Revision.SPLIT) {
			SplitRevision<K, V> splitRevision = (SplitRevision<K, V>) revision;
			// key null if looking for first key
			if (key != null && splitRevision.left && Jiffy.cpr(comparator, key, splitRevision.getValue().lastKey()) > 0)
				return splitRevision.sibling;
		}

		return revision;
	}

	protected Revision<K, V> debugGetNewestRevision(K key, Revision<K, V> head, Jiffy<K, V> map,
			List<Revision<K, V>> revisions) {
		// HeavyRevision<K, V> revision = head;
		Revision<K, V> revision = acquireRevisionHead();
		long revisionVersion = 0;

		while (revision != null) {
			revisions.add(revision);
			if (revision.getType() == Revision.MERGE_TERMINATOR) {
				revision = revision.acquireNext();
				continue;
			}

			revisionVersion = revision.getVersion();
			if (revisionVersion > 0)
				break;

			if (revision.getType() == Revision.MERGE) {
				MergeRevision<K, V> mergeRevision = (MergeRevision<K, V>) revision;
				if (Jiffy.cpr(comparator, key, mergeRevision.keyOfRightNode) >= 0) {
					revision = mergeRevision.acquireRightNext();
					continue;
				}
			}
			revision = revision.acquireNext();
		}

		if (revision != null)
			map.publishVersion(revisionVersion);

		if (revision.getType() == Revision.SPLIT) {
			SplitRevision<K, V> splitRevision = (SplitRevision<K, V>) revision;
			if (splitRevision.left && Jiffy.cpr(comparator, key, splitRevision.getValue().lastKey()) > 0)
				return splitRevision.sibling;
		}

		return revision;
	}

	protected Revision<K, V> getBatchRevision(K key, BatchDescriptor<K, V> descriptor, Revision<K, V> head,
			Jiffy<K, V> map, long[] statsArray) {
//		HeavyRevision<K, V> revision = acquireRevisionHead();
		Revision<K, V> revision = head;

		while (revision.descriptor != descriptor) {
			if (revision.getType() == Revision.MERGE) {
				MergeRevision<K, V> mergeRevision = (MergeRevision<K, V>) revision;
				if (Jiffy.cpr(comparator, key, mergeRevision.keyOfRightNode) >= 0) {
					revision = mergeRevision.acquireRightNext();

					if (revision == null)
						break;

					continue;
				}
			}

			revision = revision.acquireNext();

			if (revision == null)
				break;

//			if (revision == null) {
//				System.out.format("wrong:\n"
//						+ "- key: %s\n"
//						+ "- desc: %s\n"
//						+ "- head: %s\n", key, descriptor, head);
//				System.exit(1);
//			}
		}

		return revision;
	}

	// returns the next revision or null if it failed
	protected Revision<K, V> putRevisionSingle(Revision<K, V> revision) {
		Revision<K, V> head;
		while (true) {
			head = acquireRevisionHead();
			if (isTerminated())
				return null;

			revision.next = head;
			if (REVISION_HEAD.compareAndSet(this, head, revision))
				break;
		}

		return head;
	}

	// not optimal, reading revision.next twice (see the call)
	// returns the next revision or null if it failed
	protected boolean tryPutRevisionSingle(Revision<K, V> revision) {
		return REVISION_HEAD.compareAndSet(this, revision.next, revision);
	}

	protected Revision<K, V> putRevisionBatch(Revision<K, V> revision, long optimisticVersion,
			boolean firstHeavyRevision, Jiffy<K, V> map, boolean primaryRun) {
		Revision<K, V> ret;
		if (firstHeavyRevision)
			// ret = putFirstHeavyRevision(revision, map);
			ret = putFirstRevision(revision, map, this.key);
		else
			// ret = putSubsequentHeavyRevision(revision, optimisticVersion, map);
			ret = putSubsequentRevision(revision, optimisticVersion, map, this.key, primaryRun);

//		if (ret != null && ret.next != null && ret.next.acquireVersion() < 0)
//			throw new RuntimeException();
		return ret;
	}

	protected Revision<K, V> putFirstRevision(Revision<K, V> revision, Jiffy<K, V> map, K key) {
		Revision<K, V> head;
		while (true) {
			head = acquireRevisionHead();
			if (isTerminated())
				return null;

			long headVersion = head.effectiveVersion();
			if (headVersion < 0)
				map.helpPut(head);

			revision.next = head;
			if (REVISION_HEAD.compareAndSet(this, head, revision))
				break;
		}

		return revision;
	}

	protected Revision<K, V> putSubsequentRevision(Revision<K, V> revision, long optimisticVersion, Jiffy<K, V> map,
			K key, boolean primaryRun) {
		Revision<K, V> lastHead = null;
		Revision<K, V> head = acquireRevisionHead();

		while (true) {
			if (isTerminated())
				return null;
			Revision<K, V> currentHeavyRevision = head;

			while (currentHeavyRevision != lastHead && currentHeavyRevision != null) {
				if (currentHeavyRevision.descriptor == revision.descriptor) {
					// someone else created the revision, so that's all for now
					return currentHeavyRevision;
				}

				long revisionVersion = currentHeavyRevision.getVersion();

				if (revisionVersion >= 0 && revisionVersion < optimisticVersion) {
					// all subsequent revisions for sure have smaller versions then
					// optimisticVersion
					// so no need to look further
					break;
				}

				currentHeavyRevision = currentHeavyRevision.acquireNext();
			}

			lastHead = head;
			head = acquireRevisionHead();

			if (head == lastHead) {
				long headVersion = head.effectiveVersion();
				if (headVersion < 0)
					map.helpPut(head);

				revision.next = head;

				BatchDescriptor<K, V> descriptor = revision.descriptor;
				if (descriptor.acquireVersion() > 0) {
					// Additional check for a situation in which someone else helped the batch
					// to complete and at the same time the revision had not been found in the while
					// loop above (e.g. because it was already GC'ed). If the version is negative,
					// it means that we can proceed. If someone is quicker than us, the CAS below
					// will fail. This cannot happen to the primary thread which started the batch.
					// return revision;

					if (primaryRun)
						continue;

					return revision; // this is not the right revision, but it does not matter
				}

				if (REVISION_HEAD.compareAndSet(this, head, revision))
					return revision;

				head = acquireRevisionHead();
			}
		}
	}

	public void debugPrintAllRevisions(long cutOffVersion, StringBuilder builder) {
		var h = acquireRevisionHead();
		debugPrintAllRevisionsHelper(h, cutOffVersion, builder, 4);
	}

	private void debugPrintAllRevisionsHelper(Revision<K, V> rev, long cutOffVersion, StringBuilder builder,
			int indent) {
		for (int i = 0; i < indent; i++)
			builder.append(' ');
		builder.append(rev + "\n");
		if (rev == null)
			return;
		long ver = rev.effectiveVersion();
		if (ver > 0 && ver < cutOffVersion)
			return;
		if (rev instanceof MergeRevision<?, ?>) {
			var mergeRev = (MergeRevision<K, V>) rev;
			for (int i = 0; i < indent; i++)
				builder.append(' ');
			builder.append("LEFT:\n");
			debugPrintAllRevisionsHelper(rev.acquireNext(), cutOffVersion, builder, indent + 4);
			for (int i = 0; i < indent; i++)
				builder.append(' ');
			builder.append("RIGHT:\n");
			debugPrintAllRevisionsHelper(mergeRev.acquireRightNext(), cutOffVersion, builder, indent + 4);
		} else
			debugPrintAllRevisionsHelper(rev.acquireNext(), cutOffVersion, builder, indent);
	}

	// VarHandle mechanics
	private static final VarHandle NEXT;
	private static final VarHandle REVISION_HEAD;
	private static final VarHandle READY_TO_UNLINK;

	static {
		try {
			MethodHandles.Lookup l = MethodHandles.lookup();
			NEXT = l.findVarHandle(Node.class, "next", Node.class);
			REVISION_HEAD = l.findVarHandle(Node.class, "revisionHead", Revision.class);
			READY_TO_UNLINK = l.findVarHandle(Node.class, "readyToUnlink", boolean.class);
		} catch (ReflectiveOperationException e) {
			throw new ExceptionInInitializerError(e);
		}
	}
}