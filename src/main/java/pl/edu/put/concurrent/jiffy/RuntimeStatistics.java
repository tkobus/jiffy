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

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class RuntimeStatistics {

	static int SKIP = 0;

	int currentOp = 0;

	// We want a histogram, so for each invocation we get some value x so we add to
	// map x=1 or get(x) and increment.

	// getNewestRevision()
	Map<Long, Long> getNewestRevisionLoop = new HashMap<>();

	// getRevision()
	Map<Long, Long> getRevisionLoop = new HashMap<>();
	Map<Long, Long> getRevisionHelpPut = new HashMap<>();
	Map<Long, Long> getRevisionCreateBulkRevision = new HashMap<>();

	// getRevision() for bulk only
	Map<Long, Long> getRevisionBulkRevisionSize = new HashMap<>();

	// findNear()
	Map<Long, Long> findNearFindPredecessor = new HashMap<>();
	Map<Long, Long> findNearSearchOnLowestLevel = new HashMap<>();
	Map<Long, Long> findNearRevisionLoop = new HashMap<>();
	Map<Long, Long> findNearFoundTempSplitNode = new HashMap<>();
	Map<Long, Long> findNearFoundMergeTerminator = new HashMap<>();

	// doGet()
	Map<Long, Long> doGetOuterLoop = new HashMap<>();
	Map<Long, Long> doGetInsertionPointLoop = new HashMap<>();
	Map<Long, Long> doGetFoundTempSplitNode = new HashMap<>();
	Map<Long, Long> doGetFoundMergeTerminator = new HashMap<>();
	Map<Long, Long> doGetNextChanged = new HashMap<>();

	// doPutSingle()
	Map<Long, Long> doPutSingleOuterLoop = new HashMap<>();
	Map<Long, Long> doPutSingleInsertionPointLoop = new HashMap<>();
	Map<Long, Long> doPutSingleFoundTempSplitNode = new HashMap<>();
	Map<Long, Long> doPutSingleBIsTerminated = new HashMap<>();
	Map<Long, Long> doPutSingleNotRegularHelpPut = new HashMap<>();
	Map<Long, Long> doPutSingleNextNodeChanged = new HashMap<>();
	Map<Long, Long> doPutSingleTrySimpleUpdate = new HashMap<>();
	Map<Long, Long> doPutSingleTrySplitUpdate = new HashMap<>();
	Map<Long, Long> doPutSingleTrySplitUpdateHelpPut = new HashMap<>();

	// doRemoveSingle()
	Map<Long, Long> doRemoveSingleOuterLoop = new HashMap<>();
	Map<Long, Long> doRemoveSingleInsertionPointLoop = new HashMap<>();
	Map<Long, Long> doRemoveSingleFoundTempSplitNode = new HashMap<>();
	Map<Long, Long> doRemoveSingleBIsTerminated = new HashMap<>();
	Map<Long, Long> doRemoveSingleNotRegularHelpPut = new HashMap<>();
	Map<Long, Long> doRemoveSingleNextNodeChanged = new HashMap<>();
	Map<Long, Long> doRemoveSingleNothingToDo = new HashMap<>();
	Map<Long, Long> doRemoveSingleTrySimpleUpdate = new HashMap<>();
	Map<Long, Long> doRemoveSingleTryMergeUpdate = new HashMap<>();
	Map<Long, Long> doRemoveSingleTryMergeUpdateHelpPut = new HashMap<>();

	// helpBatchPrimaryRun()
	Map<Long, Long> helpBatchPrimaryRunLoop = new HashMap<>();
	
	// helpBatchPrimaryRun()
	Map<Long, Long> helpBatchLoop = new HashMap<>();
	
	// doPutBatch() primary run
	Map<Long, Long> doPutBatchPrimaryRunOuterLoop = new HashMap<>();
	Map<Long, Long> doPutBatchPrimaryRunInsertionPointLoop = new HashMap<>();
	Map<Long, Long> doPutBatchPrimaryRunFoundTempSplitNode = new HashMap<>();
	Map<Long, Long> doPutBatchPrimaryRunBIsTerminated = new HashMap<>();
	Map<Long, Long> doPutBatchPrimaryRunNextNodeChanged = new HashMap<>();
	Map<Long, Long> doPutBatchPrimaryRunFoundBatchsRevision = new HashMap<>();
	Map<Long, Long> doPutBatchPrimaryRunHelpPut = new HashMap<>();
	Map<Long, Long> doPutBatchPrimaryRunNextNodeChangedSecond = new HashMap<>();
	Map<Long, Long> doPutBatchPrimaryRunFinalVersionSet = new HashMap<>();
	Map<Long, Long> doPutBatchPrimaryRunTrySimpleUpdate = new HashMap<>();
	Map<Long, Long> doPutBatchPrimaryRunTrySplitUpdate = new HashMap<>();
	Map<Long, Long> doPutBatchPrimaryRunTryMergeUpdate = new HashMap<>();
	
	// doPutBatch() helper run
	Map<Long, Long> doPutBatchHelperRunOuterLoop = new HashMap<>();
	Map<Long, Long> doPutBatchHelperRunInsertionPointLoop = new HashMap<>();
	Map<Long, Long> doPutBatchHelperRunFoundTempSplitNode = new HashMap<>();
	Map<Long, Long> doPutBatchHelperRunBIsTerminated = new HashMap<>();
	Map<Long, Long> doPutBatchHelperRunNextNodeChanged = new HashMap<>();
	Map<Long, Long> doPutBatchHelperRunFoundBatchsRevision = new HashMap<>();
	Map<Long, Long> doPutBatchHelperRunHelpPut = new HashMap<>();
	Map<Long, Long> doPutBatchHelperRunNextNodeChangedSecond = new HashMap<>();
	Map<Long, Long> doPutBatchHelperRunFinalVersionSet = new HashMap<>();
	Map<Long, Long> doPutBatchHelperRunTrySimpleUpdate = new HashMap<>();
	Map<Long, Long> doPutBatchHelperRunTrySplitUpdate = new HashMap<>();
	Map<Long, Long> doPutBatchHelperRunTryMergeUpdate = new HashMap<>();
	
	// findMergeRevision()
	Map<Long, Long> findMergeRevisionForRemoveOuterLoop = new HashMap<>();
	Map<Long, Long> findMergeRevisionForRemoveInsertionPointLoop = new HashMap<>();
	Map<Long, Long> findMergeRevisionForRemoveFoundTempSplitNode = new HashMap<>();
	Map<Long, Long> findMergeRevisionForRemoveBIsTerminated = new HashMap<>();
	Map<Long, Long> findMergeRevisionForRemoveHelpMergeRevision = new HashMap<>();
	Map<Long, Long> findMergeRevisionForRemoveRevisionLoop = new HashMap<>();

	Map<Long, Long> findMergeRevisionForOtherOuterLoop = new HashMap<>();
	Map<Long, Long> findMergeRevisionForOtherInsertionPointLoop = new HashMap<>();
	Map<Long, Long> findMergeRevisionForOtherFoundTempSplitNode = new HashMap<>();
	Map<Long, Long> findMergeRevisionForOtherBIsTerminated = new HashMap<>();
	Map<Long, Long> findMergeRevisionForOtherHelpMergeRevision = new HashMap<>();
	Map<Long, Long> findMergeRevisionForOtherRevisionLoop = new HashMap<>();

	// helpMerge()
	Map<Long, Long> helpMergeVersionAlreadySet = new HashMap<>();

	// helpMergeBatch()
	Map<Long, Long> helpMergeBatchVersionAlreadySet = new HashMap<>();

	// helpMergeTerminator()
	Map<Long, Long> helpMergeTerminatorOuterLoop = new HashMap<>();
	Map<Long, Long> helpMergeTerminatorOuterLoopBIsTerminated = new HashMap<>();
	Map<Long, Long> helpMergeTerminatorOuterLoopBBNull = new HashMap<>();
	Map<Long, Long> helpMergeTerminatorInnerLoop = new HashMap<>();
	Map<Long, Long> helpMergeTerminatorInnerLoopBBTooFar = new HashMap<>();
	Map<Long, Long> helpMergeTerminatorInnerLoopBBTooFarForcedRestart = new HashMap<>();
	Map<Long, Long> helpMergeTerminatorInnerLoopBBTerminated = new HashMap<>();
	Map<Long, Long> helpMergeTerminatorInnerLoopBBTerminatedNoBBB = new HashMap<>();
	Map<Long, Long> helpMergeTerminatorInnerLoopFoundTempSplitNodeNoBBB = new HashMap<>();
	Map<Long, Long> helpMergeTerminatorInnerLoopFoundTempSplitNodeHelpBBB = new HashMap<>();
	Map<Long, Long> helpMergeTerminatorOtherHelpPut = new HashMap<>();
	Map<Long, Long> helpMergeTerminatorBIsTerminated = new HashMap<>();
	Map<Long, Long> helpMergeTerminatorBBNextAndBDontMatch = new HashMap<>();
	Map<Long, Long> helpMergeTerminatorHeadEmpty = new HashMap<>();
	Map<Long, Long> helpMergeTerminatorMergeRevisionAddedAndHelp = new HashMap<>();

	// helpMergeTerminatorBatch()
	Map<Long, Long> helpMergeTerminatorBatchOuterLoop = new HashMap<>();
	Map<Long, Long> helpMergeTerminatorBatchOuterLoopBIsTerminated = new HashMap<>();
	Map<Long, Long> helpMergeTerminatorBatchOuterLoopBBNull = new HashMap<>();
	Map<Long, Long> helpMergeTerminatorBatchInnerLoop = new HashMap<>();
	Map<Long, Long> helpMergeTerminatorBatchInnerLoopBBTooFar = new HashMap<>();
	Map<Long, Long> helpMergeTerminatorBatchInnerLoopBBTooFarForcedRestart = new HashMap<>();
	Map<Long, Long> helpMergeTerminatorBatchInnerLoopBBTerminated = new HashMap<>();
	Map<Long, Long> helpMergeTerminatorBatchInnerLoopBBTerminatedNoBBB = new HashMap<>();
	Map<Long, Long> helpMergeTerminatorBatchInnerLoopFoundTempSplitNodeNoBBB = new HashMap<>();
	Map<Long, Long> helpMergeTerminatorBatchInnerLoopFoundTempSplitNodeHelpBBB = new HashMap<>(); // 9
	Map<Long, Long> helpMergeTerminatorBatchBIsTerminated = new HashMap<>(); // new
	Map<Long, Long> helpMergeTerminatorBatchHelpMyMerge = new HashMap<>(); // new
	Map<Long, Long> helpMergeTerminatorBatchOtherHelpPut = new HashMap<>();
	Map<Long, Long> helpMergeTerminatorBatchBIsTerminatedSecond = new HashMap<>(); // changed
	Map<Long, Long> helpMergeTerminatorBatchBBNextAndBDontMatch = new HashMap<>();
	Map<Long, Long> helpMergeTerminatorBatchHeadEmpty = new HashMap<>();
	Map<Long, Long> helpMergeTerminatorBatchMergeRevisionAddedAndHelp = new HashMap<>();

	// helpTempSplitNode()
	Map<Long, Long> helpTempSplitNodeVersionAlreadySet = new HashMap<>();
	Map<Long, Long> helpTempSplitNodeVersionAlreadySetFullCleanup = new HashMap<>();

	// helpTempSplitNodeBatch()
	Map<Long, Long> helpTempSplitNodeBatchVersionAlreadySet = new HashMap<>();
	Map<Long, Long> helpTempSplitNodeBatchVersionAlreadySetFullCleanup = new HashMap<>();

	// helpSplit()
	Map<Long, Long> helpSplitLeftRevision = new HashMap<>();
	Map<Long, Long> helpSplitLeftRevisionOuterLoop = new HashMap<>();
	Map<Long, Long> helpSplitLeftRevisionBIsTerminated = new HashMap<>();
	Map<Long, Long> helpSplitLeftRevisionVersionIsSetOnBHead = new HashMap<>();
	Map<Long, Long> helpSplitLeftRevisionInnerLoop = new HashMap<>();
	Map<Long, Long> helpSplitLeftRevisionInnerLoopFoundRightRevision = new HashMap<>();
	Map<Long, Long> helpSplitLeftRevisionInnerLoopVersionIsSetOnLeftRevision = new HashMap<>();
	Map<Long, Long> helpSplitLeftRevisionInnerLoopSplitNodeAlreadyAdded = new HashMap<>();
	Map<Long, Long> helpSplitLeftRevisionInnerLoopSplitNodeCassedIn = new HashMap<>();
	Map<Long, Long> helpSplitLeftRevisionBreakFromABA = new HashMap<>();
	Map<Long, Long> helpSplitLeftRevisionProperNodeCassedIn = new HashMap<>();

	// helpSplitBatch()
	Map<Long, Long> helpSplitBatchLeftRevision = new HashMap<>();
	Map<Long, Long> helpSplitBatchLeftRevisionOuterLoop = new HashMap<>();
	Map<Long, Long> helpSplitBatchLeftRevisionBIsTerminated = new HashMap<>();
	Map<Long, Long> helpSplitBatchLeftRevisionVersionIsSetOnBHead = new HashMap<>();
	Map<Long, Long> helpSplitBatchLeftRevisionInnerLoop = new HashMap<>();
	Map<Long, Long> helpSplitBatchLeftRevisionInnerLoopFoundRightRevision = new HashMap<>();
	Map<Long, Long> helpSplitBatchLeftRevisionInnerLoopVersionIsSetOnLeftRevision = new HashMap<>();
	Map<Long, Long> helpSplitBatchLeftRevisionInnerLoopSplitNodeAlreadyAdded = new HashMap<>();
	Map<Long, Long> helpSplitBatchLeftRevisionInnerLoopSplitNodeCassedIn = new HashMap<>();
	Map<Long, Long> helpSplitBatchLeftRevisionBreakFromABA = new HashMap<>();
	Map<Long, Long> helpSplitBatchLeftRevisionProperNodeCassedIn = new HashMap<>();

	// doGc()
	Map<Long, Long> gcRegularGcDepth = new HashMap<>();
	Map<Long, Long> gcInnerGcDepth = new HashMap<>();

	// iterator
	Map<Long, Long> iterAscendOuterLoop = new HashMap<>();
	Map<Long, Long> iterAscendSkipTempSplitNode = new HashMap<>();
	Map<Long, Long> iterAscendRevisionFound = new HashMap<>();

	// SubMapIter.ascend()
	Map<Long, Long> subMapIterAscendOuterLoop = new HashMap<>();
	Map<Long, Long> subMapIterAscendInnerLoop = new HashMap<>();
	Map<Long, Long> subMapIterAscendInnerLoopFoundTempSplitNode = new HashMap<>();
	Map<Long, Long> subMapIterAscendInnerLoopFoundMergeTerminator = new HashMap<>();
	Map<Long, Long> subMapIterAscendRevisionFound = new HashMap<>();

	// SubMapIter.descend()
	Map<Long, Long> subMapIterDescendOuterLoop = new HashMap<>();
	Map<Long, Long> subMapIterDescendFindNear = new HashMap<>();

	// SubMap.forEach()
	Map<Long, Long> subMapForEachAscendOuterLoop = new HashMap<>();
	Map<Long, Long> subMapForEachAscendWillBeMore = new HashMap<>();
	Map<Long, Long> subMapForEachAscendWontBeMoreNextNull = new HashMap<>();
	Map<Long, Long> subMapForEachAscendWontBeMoreTooHigh = new HashMap<>();
	Map<Long, Long> subMapForEachAscendPerformAcceptLoop = new HashMap<>();
	Map<Long, Long> subMapForEachAscendInnerLoop = new HashMap<>();
	Map<Long, Long> subMapForEachAscendInnerLoopFoundTempSplitNode = new HashMap<>();
	Map<Long, Long> subMapForEachAscendInnerLoopFoundMergeTerminator = new HashMap<>();
	Map<Long, Long> subMapForEachAscendRevisionFound = new HashMap<>();
	
	public RuntimeStatistics() {

	}

	private boolean skip() {
		currentOp++;
		if (SKIP == 0)
			return false;
		return currentOp % SKIP != 0;
	}

	public void updateGetNewestRevision(long[] statsArray) {
		if (skip())
			return;

		long loops = statsArray[0];

		Long count = getNewestRevisionLoop.get(loops);
		if (count == null)
			getNewestRevisionLoop.put(loops, 1l);
		else
			getNewestRevisionLoop.put(loops, count + 1);
	}

	private void updateMap(Map<Long, Long> map, long count) {
		Long oldEvents = (Long) map.get(count);
		if (oldEvents == null)
			map.put(count, 1l);
		else
			map.put(count, oldEvents + 1);
	}

	public void updateGetRevision(long[] statsArray) {
		if (skip())
			return;

		@SuppressWarnings("unchecked")
		Map<Long, Long>[] maps = new Map[] { getRevisionLoop, getRevisionHelpPut, getRevisionCreateBulkRevision };

		for (int i = 0; i < statsArray.length; i++)
			updateMap(maps[i], statsArray[i]);
	}

	public void updateGetRevisionBulkOnly(long[] statsArray) {
		if (skip())
			return;

		@SuppressWarnings("unchecked")
		Map<Long, Long>[] maps = new Map[] { getRevisionBulkRevisionSize };

		for (int i = 0; i < statsArray.length; i++)
			updateMap(maps[i], statsArray[i]);
	}	

	public void updateFindNear(long[] statsArray) {
		if (skip())
			return;

		@SuppressWarnings("unchecked")
		Map<Long, Long>[] maps = new Map[] { findNearFindPredecessor, findNearSearchOnLowestLevel, findNearRevisionLoop,
				findNearFoundTempSplitNode, findNearFoundMergeTerminator };

		for (int i = 0; i < statsArray.length; i++)
			updateMap(maps[i], statsArray[i]);
	}

	public void updateDoGet(long[] statsArray) {
		if (skip())
			return;

		@SuppressWarnings("unchecked")
		Map<Long, Long>[] maps = new Map[] { doGetOuterLoop, doGetInsertionPointLoop, doGetFoundTempSplitNode,
				doGetFoundMergeTerminator, doGetNextChanged };

		for (int i = 0; i < statsArray.length; i++)
			updateMap(maps[i], statsArray[i]);
	}

	public void updateDoPutSingle(long[] statsArray) {
		if (skip())
			return;

		@SuppressWarnings("unchecked")
		Map<Long, Long>[] maps = new Map[] { doPutSingleOuterLoop, doPutSingleInsertionPointLoop, doPutSingleFoundTempSplitNode,
				doPutSingleBIsTerminated, doPutSingleNotRegularHelpPut, doPutSingleNextNodeChanged,
				doPutSingleTrySimpleUpdate, doPutSingleTrySplitUpdate, doPutSingleTrySplitUpdateHelpPut };

		for (int i = 0; i < statsArray.length; i++)
			updateMap(maps[i], statsArray[i]);
	}

	public void updateDoRemoveSingle(long[] statsArray) {
		if (skip())
			return;

		@SuppressWarnings("unchecked")
		Map<Long, Long>[] maps = new Map[] { doRemoveSingleOuterLoop, doRemoveSingleInsertionPointLoop,
				doRemoveSingleFoundTempSplitNode, doRemoveSingleBIsTerminated, doRemoveSingleNotRegularHelpPut,
				doRemoveSingleNextNodeChanged, doRemoveSingleNothingToDo, doRemoveSingleTrySimpleUpdate,
				doRemoveSingleTryMergeUpdate, doRemoveSingleTryMergeUpdateHelpPut };

		for (int i = 0; i < statsArray.length; i++)
			updateMap(maps[i], statsArray[i]);
	}

	public void updateHelpBatchPrimaryRun(long[] statsArray) {
		if (skip())
			return;

		@SuppressWarnings("unchecked")
		Map<Long, Long>[] maps = new Map[] { helpBatchPrimaryRunLoop };

		for (int i = 0; i < statsArray.length; i++)
			updateMap(maps[i], statsArray[i]);
	}
	
	public void updateHelpBatch(long[] statsArray) {
		if (skip())
			return;

		@SuppressWarnings("unchecked")
		Map<Long, Long>[] maps = new Map[] { helpBatchLoop };

		for (int i = 0; i < statsArray.length; i++)
			updateMap(maps[i], statsArray[i]);
	}
	
	public void updateDoPutBatchPrimaryRun(long[] statsArray) {
		if (skip())
			return;

		@SuppressWarnings("unchecked")
		Map<Long, Long>[] maps = new Map[] { doPutBatchPrimaryRunOuterLoop,
				doPutBatchPrimaryRunInsertionPointLoop,
				doPutBatchPrimaryRunFoundTempSplitNode,
				doPutBatchPrimaryRunBIsTerminated,
				doPutBatchPrimaryRunNextNodeChanged,
				doPutBatchPrimaryRunFoundBatchsRevision,
				doPutBatchPrimaryRunHelpPut,
				doPutBatchPrimaryRunNextNodeChangedSecond,
				doPutBatchPrimaryRunFinalVersionSet,
				doPutBatchPrimaryRunTrySimpleUpdate,
				doPutBatchPrimaryRunTrySplitUpdate,
				doPutBatchPrimaryRunTryMergeUpdate };

		for (int i = 0; i < statsArray.length; i++)
			updateMap(maps[i], statsArray[i]);
	}

	public void updateDoPutBatchHelperRun(long[] statsArray) {
		if (skip())
			return;

		@SuppressWarnings("unchecked")
		Map<Long, Long>[] maps = new Map[] {doPutBatchHelperRunOuterLoop,
				doPutBatchHelperRunInsertionPointLoop,
				doPutBatchHelperRunFoundTempSplitNode,
				doPutBatchHelperRunBIsTerminated,
				doPutBatchHelperRunNextNodeChanged,
				doPutBatchHelperRunFoundBatchsRevision,
				doPutBatchHelperRunHelpPut,
				doPutBatchHelperRunNextNodeChangedSecond,
				doPutBatchHelperRunFinalVersionSet,
				doPutBatchHelperRunTrySimpleUpdate,
				doPutBatchHelperRunTrySplitUpdate,
				doPutBatchHelperRunTryMergeUpdate };

		for (int i = 0; i < statsArray.length; i++)
			updateMap(maps[i], statsArray[i]);
	}
		
	@SuppressWarnings("unchecked")
	public void updateFindMergeTerminator(long[] statsArray, boolean doRemoveSingleRun) {
		if (skip())
			return;
		
		Map<Long, Long>[] maps;
		if (doRemoveSingleRun) {
			maps = new Map[] { findMergeRevisionForRemoveOuterLoop, findMergeRevisionForRemoveInsertionPointLoop,
					findMergeRevisionForRemoveFoundTempSplitNode, findMergeRevisionForRemoveBIsTerminated,
					findMergeRevisionForRemoveHelpMergeRevision, findMergeRevisionForRemoveRevisionLoop };
		} else {
			maps = new Map[] { findMergeRevisionForOtherOuterLoop, findMergeRevisionForOtherInsertionPointLoop,
					findMergeRevisionForOtherFoundTempSplitNode, findMergeRevisionForOtherBIsTerminated,
					findMergeRevisionForOtherHelpMergeRevision, findMergeRevisionForOtherRevisionLoop };
		}

		for (int i = 0; i < statsArray.length; i++)
			updateMap(maps[i], statsArray[i]);
	}

	public void updateHelpMerge(long[] statsArray) {
		if (skip())
			return;

		@SuppressWarnings("unchecked")
		Map<Long, Long>[] maps = new Map[] { helpMergeVersionAlreadySet };

		for (int i = 0; i < statsArray.length; i++)
			updateMap(maps[i], statsArray[i]);
	}

	public void updateHelpMergeBatch(long[] statsArray) {
		if (skip())
			return;

		@SuppressWarnings("unchecked")
		Map<Long, Long>[] maps = new Map[] { helpMergeBatchVersionAlreadySet };

		for (int i = 0; i < statsArray.length; i++)
			updateMap(maps[i], statsArray[i]);
	}

	public void updateHelpMergeTerminator(long[] statsArray) {
		if (skip())
			return;

		@SuppressWarnings("unchecked")
		Map<Long, Long>[] maps = new Map[] { helpMergeTerminatorOuterLoop, helpMergeTerminatorOuterLoopBIsTerminated,
				helpMergeTerminatorOuterLoopBBNull, helpMergeTerminatorInnerLoop, helpMergeTerminatorInnerLoopBBTooFar,
				helpMergeTerminatorInnerLoopBBTooFarForcedRestart, helpMergeTerminatorInnerLoopBBTerminated,
				helpMergeTerminatorInnerLoopBBTerminatedNoBBB, helpMergeTerminatorInnerLoopFoundTempSplitNodeNoBBB,
				helpMergeTerminatorInnerLoopFoundTempSplitNodeHelpBBB, helpMergeTerminatorOtherHelpPut,
				helpMergeTerminatorBIsTerminated, helpMergeTerminatorBBNextAndBDontMatch, helpMergeTerminatorHeadEmpty,
				helpMergeTerminatorMergeRevisionAddedAndHelp };

		for (int i = 0; i < statsArray.length; i++)
			updateMap(maps[i], statsArray[i]);
	}

	public void updateHelpMergeTerminatorBatch(long[] statsArray) {
		if (skip())
			return;

		@SuppressWarnings("unchecked")
		Map<Long, Long>[] maps = new Map[] { helpMergeTerminatorBatchOuterLoop, helpMergeTerminatorBatchOuterLoopBIsTerminated,
				helpMergeTerminatorBatchOuterLoopBBNull, helpMergeTerminatorBatchInnerLoop,
				helpMergeTerminatorBatchInnerLoopBBTooFar, helpMergeTerminatorBatchInnerLoopBBTooFarForcedRestart,
				helpMergeTerminatorBatchInnerLoopBBTerminated, helpMergeTerminatorBatchInnerLoopBBTerminatedNoBBB,
				helpMergeTerminatorBatchInnerLoopFoundTempSplitNodeNoBBB,
				helpMergeTerminatorBatchInnerLoopFoundTempSplitNodeHelpBBB, helpMergeTerminatorBatchBIsTerminated,
				helpMergeTerminatorBatchHelpMyMerge, helpMergeTerminatorBatchOtherHelpPut,
				helpMergeTerminatorBatchBIsTerminatedSecond, helpMergeTerminatorBatchBBNextAndBDontMatch,
				helpMergeTerminatorBatchHeadEmpty, helpMergeTerminatorBatchMergeRevisionAddedAndHelp };

		for (int i = 0; i < statsArray.length; i++)
			updateMap(maps[i], statsArray[i]);
	}

	public void updateHelpTempSplitNode(long[] statsArray) {
		if (skip())
			return;

		@SuppressWarnings("unchecked")
		Map<Long, Long>[] maps = new Map[] { helpTempSplitNodeVersionAlreadySet, helpTempSplitNodeVersionAlreadySetFullCleanup };

		for (int i = 0; i < statsArray.length; i++)
			updateMap(maps[i], statsArray[i]);
	}

	public void updateHelpTempSplitNodeBatch(long[] statsArray) {
		if (skip())
			return;

		@SuppressWarnings("unchecked")
		Map<Long, Long>[] maps = new Map[] { helpTempSplitNodeBatchVersionAlreadySet,
				helpTempSplitNodeBatchVersionAlreadySetFullCleanup };

		for (int i = 0; i < statsArray.length; i++)
			updateMap(maps[i], statsArray[i]);
	}

	public void updateHelpSplit(long[] statsArray) {
		if (skip())
			return;

		@SuppressWarnings("unchecked")
		Map<Long, Long>[] maps = new Map[] { helpSplitLeftRevision, helpSplitLeftRevisionOuterLoop,
				helpSplitLeftRevisionBIsTerminated, helpSplitLeftRevisionVersionIsSetOnBHead,
				helpSplitLeftRevisionInnerLoop, helpSplitLeftRevisionInnerLoopFoundRightRevision,
				helpSplitLeftRevisionInnerLoopVersionIsSetOnLeftRevision,
				helpSplitLeftRevisionInnerLoopSplitNodeAlreadyAdded, helpSplitLeftRevisionInnerLoopSplitNodeCassedIn,
				helpSplitLeftRevisionBreakFromABA, helpSplitLeftRevisionProperNodeCassedIn };

		for (int i = 0; i < statsArray.length; i++)
			updateMap(maps[i], statsArray[i]);
	}

	public void updateHelpSplitBatch(long[] statsArray) {
		if (skip())
			return;

		@SuppressWarnings("unchecked")
		Map<Long, Long>[] maps = new Map[] { helpSplitBatchLeftRevision, helpSplitBatchLeftRevisionOuterLoop,
				helpSplitBatchLeftRevisionBIsTerminated, helpSplitBatchLeftRevisionVersionIsSetOnBHead,
				helpSplitBatchLeftRevisionInnerLoop, helpSplitBatchLeftRevisionInnerLoopFoundRightRevision,
				helpSplitBatchLeftRevisionInnerLoopVersionIsSetOnLeftRevision,
				helpSplitBatchLeftRevisionInnerLoopSplitNodeAlreadyAdded, helpSplitBatchLeftRevisionInnerLoopSplitNodeCassedIn,
				helpSplitBatchLeftRevisionBreakFromABA, helpSplitBatchLeftRevisionProperNodeCassedIn };

		for (int i = 0; i < statsArray.length; i++)
			updateMap(maps[i], statsArray[i]);
	}
	
	public void updateGC(long[] statsArray) {
		if (skip())
			return;

		@SuppressWarnings("unchecked")
		Map<Long, Long>[] maps = new Map[] { gcRegularGcDepth, gcInnerGcDepth };

		for (int i = 0; i < statsArray.length; i++)
			updateMap(maps[i], statsArray[i]);
	}

	// currently not used, iterators are handled through submap iterators
	public void updateIterAscend(long[] statsArray) {
		if (skip())
			return;

		@SuppressWarnings("unchecked")
		Map<Long, Long>[] maps = new Map[] { iterAscendOuterLoop, iterAscendSkipTempSplitNode, iterAscendRevisionFound };

		for (int i = 0; i < statsArray.length; i++)
			updateMap(maps[i], statsArray[i]);
	}

	public void updateSubMapIterAscend(long[] statsArray) {
		if (skip())
			return;

		@SuppressWarnings("unchecked")
		Map<Long, Long>[] maps = new Map[] { subMapIterAscendOuterLoop, subMapIterAscendInnerLoop,
				subMapIterAscendInnerLoopFoundTempSplitNode, subMapIterAscendInnerLoopFoundMergeTerminator,
				subMapIterAscendRevisionFound };

		for (int i = 0; i < statsArray.length; i++)
			updateMap(maps[i], statsArray[i]);
	}

	public void updateSubMapIterDescend(long[] statsArray) {
		if (skip())
			return;

		@SuppressWarnings("unchecked")
		Map<Long, Long>[] maps = new Map[] { subMapIterDescendOuterLoop, subMapIterDescendFindNear };

		for (int i = 0; i < statsArray.length; i++)
			updateMap(maps[i], statsArray[i]);
	}
	
	public void updateSubMapForEach(long[] statsArray) {
		if (skip())
			return;

		@SuppressWarnings("unchecked")
		Map<Long, Long>[] maps = new Map[] { subMapForEachAscendOuterLoop,
				subMapForEachAscendWillBeMore,
				subMapForEachAscendWontBeMoreNextNull,
				subMapForEachAscendWontBeMoreTooHigh,
				subMapForEachAscendPerformAcceptLoop,
				subMapForEachAscendInnerLoop,
				subMapForEachAscendInnerLoopFoundTempSplitNode,
				subMapForEachAscendInnerLoopFoundMergeTerminator,
				subMapForEachAscendRevisionFound };

		for (int i = 0; i < statsArray.length; i++)
			updateMap(maps[i], statsArray[i]);		
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();

		Comparator<Map.Entry<Long, Long>> cmp = (a, b) -> Long.compare(b.getValue(), a.getValue());

		@SuppressWarnings("unchecked")
		Map<Long, Long>[] maps = new Map[] { getNewestRevisionLoop, getRevisionLoop, getRevisionHelpPut,
				getRevisionCreateBulkRevision, 
				getRevisionBulkRevisionSize,			
				findNearFindPredecessor, findNearSearchOnLowestLevel,
				findNearRevisionLoop, findNearFoundTempSplitNode, findNearFoundMergeTerminator, doGetOuterLoop,
				doGetInsertionPointLoop, doGetFoundTempSplitNode, doGetFoundMergeTerminator, doGetNextChanged,
				doPutSingleOuterLoop, doPutSingleInsertionPointLoop, doPutSingleFoundTempSplitNode,
				doPutSingleBIsTerminated, doPutSingleNotRegularHelpPut, doPutSingleNextNodeChanged,
				doPutSingleTrySimpleUpdate, doPutSingleTrySplitUpdate, doPutSingleTrySplitUpdateHelpPut,
				doRemoveSingleOuterLoop, doRemoveSingleInsertionPointLoop, doRemoveSingleFoundTempSplitNode,
				doRemoveSingleBIsTerminated, doRemoveSingleNotRegularHelpPut, doRemoveSingleNextNodeChanged,
				doRemoveSingleNothingToDo, doRemoveSingleTrySimpleUpdate, doRemoveSingleTryMergeUpdate,
				doRemoveSingleTryMergeUpdateHelpPut, 
				helpBatchPrimaryRunLoop,
				helpBatchLoop,			
				 doPutBatchPrimaryRunOuterLoop,
					doPutBatchPrimaryRunInsertionPointLoop,
					doPutBatchPrimaryRunFoundTempSplitNode,
					doPutBatchPrimaryRunBIsTerminated,
					doPutBatchPrimaryRunNextNodeChanged,
					doPutBatchPrimaryRunFoundBatchsRevision,
					doPutBatchPrimaryRunHelpPut,
					doPutBatchPrimaryRunNextNodeChangedSecond,
					doPutBatchPrimaryRunFinalVersionSet,
					doPutBatchPrimaryRunTrySimpleUpdate,
					doPutBatchPrimaryRunTrySplitUpdate,
					doPutBatchPrimaryRunTryMergeUpdate,
				
					doPutBatchHelperRunOuterLoop,
					doPutBatchHelperRunInsertionPointLoop,
					doPutBatchHelperRunFoundTempSplitNode,
					doPutBatchHelperRunBIsTerminated,
					doPutBatchHelperRunNextNodeChanged,
					doPutBatchHelperRunFoundBatchsRevision,
					doPutBatchHelperRunHelpPut,
					doPutBatchHelperRunNextNodeChangedSecond,
					doPutBatchHelperRunFinalVersionSet,
					doPutBatchHelperRunTrySimpleUpdate,
					doPutBatchHelperRunTrySplitUpdate,
					doPutBatchHelperRunTryMergeUpdate,
				findMergeRevisionForRemoveOuterLoop,
				findMergeRevisionForRemoveInsertionPointLoop, findMergeRevisionForRemoveFoundTempSplitNode,
				findMergeRevisionForRemoveBIsTerminated, findMergeRevisionForRemoveHelpMergeRevision,
				findMergeRevisionForRemoveRevisionLoop, findMergeRevisionForOtherOuterLoop,
				findMergeRevisionForOtherInsertionPointLoop, findMergeRevisionForOtherFoundTempSplitNode,
				findMergeRevisionForOtherBIsTerminated, findMergeRevisionForOtherHelpMergeRevision,
				findMergeRevisionForOtherRevisionLoop, helpMergeVersionAlreadySet, helpMergeBatchVersionAlreadySet,
				helpMergeTerminatorOuterLoop, helpMergeTerminatorOuterLoopBIsTerminated,
				helpMergeTerminatorOuterLoopBBNull, helpMergeTerminatorInnerLoop, helpMergeTerminatorInnerLoopBBTooFar,
				helpMergeTerminatorInnerLoopBBTooFarForcedRestart, helpMergeTerminatorInnerLoopBBTerminated,
				helpMergeTerminatorInnerLoopBBTerminatedNoBBB, helpMergeTerminatorInnerLoopFoundTempSplitNodeNoBBB,
				helpMergeTerminatorInnerLoopFoundTempSplitNodeHelpBBB, helpMergeTerminatorOtherHelpPut,
				helpMergeTerminatorBIsTerminated, helpMergeTerminatorBBNextAndBDontMatch, helpMergeTerminatorHeadEmpty,
				helpMergeTerminatorMergeRevisionAddedAndHelp, helpMergeTerminatorBatchOuterLoop,
				helpMergeTerminatorBatchOuterLoopBIsTerminated, helpMergeTerminatorBatchOuterLoopBBNull,
				helpMergeTerminatorBatchInnerLoop, helpMergeTerminatorBatchInnerLoopBBTooFar,
				helpMergeTerminatorBatchInnerLoopBBTooFarForcedRestart, helpMergeTerminatorBatchInnerLoopBBTerminated,
				helpMergeTerminatorBatchInnerLoopBBTerminatedNoBBB,
				helpMergeTerminatorBatchInnerLoopFoundTempSplitNodeNoBBB,
				helpMergeTerminatorBatchInnerLoopFoundTempSplitNodeHelpBBB, helpMergeTerminatorBatchBIsTerminated,
				helpMergeTerminatorBatchHelpMyMerge, helpMergeTerminatorBatchOtherHelpPut,
				helpMergeTerminatorBatchBIsTerminatedSecond, helpMergeTerminatorBatchBBNextAndBDontMatch,
				helpMergeTerminatorBatchHeadEmpty, helpMergeTerminatorBatchMergeRevisionAddedAndHelp,
				helpTempSplitNodeVersionAlreadySet, helpTempSplitNodeVersionAlreadySetFullCleanup,
				helpTempSplitNodeBatchVersionAlreadySet, helpTempSplitNodeBatchVersionAlreadySetFullCleanup,
				helpSplitLeftRevision, helpSplitLeftRevisionOuterLoop,
				helpSplitLeftRevisionBIsTerminated, helpSplitLeftRevisionVersionIsSetOnBHead,
				helpSplitLeftRevisionInnerLoop, helpSplitLeftRevisionInnerLoopFoundRightRevision,
				helpSplitLeftRevisionInnerLoopVersionIsSetOnLeftRevision,
				helpSplitLeftRevisionInnerLoopSplitNodeAlreadyAdded, helpSplitLeftRevisionInnerLoopSplitNodeCassedIn,
				helpSplitLeftRevisionBreakFromABA, helpSplitLeftRevisionProperNodeCassedIn,
				helpSplitBatchLeftRevision, helpSplitBatchLeftRevisionOuterLoop,
				helpSplitBatchLeftRevisionBIsTerminated, helpSplitBatchLeftRevisionVersionIsSetOnBHead,
				helpSplitBatchLeftRevisionInnerLoop, helpSplitBatchLeftRevisionInnerLoopFoundRightRevision,
				helpSplitBatchLeftRevisionInnerLoopVersionIsSetOnLeftRevision,
				helpSplitBatchLeftRevisionInnerLoopSplitNodeAlreadyAdded, helpSplitBatchLeftRevisionInnerLoopSplitNodeCassedIn,
				helpSplitBatchLeftRevisionBreakFromABA, helpSplitBatchLeftRevisionProperNodeCassedIn,
				
				gcRegularGcDepth, gcInnerGcDepth,
				// iterAscendOuterLoop, iterAscendSkipTempSplitNode, iterAscendRevisionFound,
				subMapIterAscendOuterLoop, subMapIterAscendInnerLoop, subMapIterAscendInnerLoopFoundTempSplitNode,
				subMapIterAscendInnerLoopFoundMergeTerminator, subMapIterAscendRevisionFound,
				subMapIterDescendOuterLoop, subMapIterDescendFindNear,
				subMapForEachAscendOuterLoop,
				subMapForEachAscendWillBeMore,
				subMapForEachAscendWontBeMoreNextNull,
				subMapForEachAscendWontBeMoreTooHigh,
				subMapForEachAscendPerformAcceptLoop,
				subMapForEachAscendInnerLoop,
				subMapForEachAscendInnerLoopFoundTempSplitNode,
				subMapForEachAscendInnerLoopFoundMergeTerminator,
				subMapForEachAscendRevisionFound 
		};
		String[] descriptions = new String[] { "getNewestRevision() major loop", "getRevision() major loop",
				"getRevision() help put", "getRevision() create bulk revisions", 
				"getRevision() bulk only, bulk size",
				"findNear() find predecessor",
				"findNear() search on lowest level", "findNear() revision loop", "findNear() found TempSplitNode",
				"findNear() found MergeTerminator", "doGet() outer loop", "doGet() insertionPoint loop",
				"doGet() found TempSplitNode", "doGet() found MergeTerminator", "doGet() next node changed",
				"doPutSingle() outer loop", 
				"doPutSingle() insertionPoint loop", 
				"doPutSingle() found TempSplitNode",
				"doPutSingle() b is terminated", 
				"doPutSingle() not regular revision help put",
				"doPutSingle() next node changed", 
				"doPutSingle() try simple update", 
				"doPutSingle() try split update",
				"doPutSingle() try split update help put", 
				"doRemoveSingle() outer loop",
				"doRemoveSingle() insertionPoint loop", "doRemoveSingle() found TempSplitNode",
				"doRemoveSingle() b is terminated", "doRemoveSingle() not regular revision help put",
				"doRemoveSingle() next node changed", "doRemoveSingle() nothing to do",
				"doRemoveSingle() try simple update", "doRemoveSingle() try merge update",
				"doRemoveSingle() try merge update help put", 
				"helpBatchPrimaryRun() main loop (corresponds to number of revisions inserted/found)",
				"helpBatch() main loop (helped at least one revision, corresponds to the number of revisions helped additionally)",
				
				"doPutBatch() (primary run) outer loop", 
				"doPutBatch() (primary run) insertionPoint loop", 
				"doPutBatch() (primary run) found TempSplitNode",
				"doPutBatch() (primary run) b is terminated", 
				"doPutBatch() (primary run) next node changed",
				"doPutBatch() (primary run) found a revision from this batch",
				"doPutBatch() (primary run) help put",
				"doPutBatch() (primary run) next node changed (second)",
				"doPutBatch() (primary run) final version already set",
				"doPutBatch() (primary run) try simple update", 
				"doPutBatch() (primary run) try split update",
				"doPutBatch() (primary run) try merge update",
				
				"doPutBatch() (helper run) outer loop", 
				"doPutBatch() (helper run) insertionPoint loop", 
				"doPutBatch() (helper run) found TempSplitNode",
				"doPutBatch() (helper run) b is terminated", 
				"doPutBatch() (helper run) next node changed",
				"doPutBatch() (helper run) found a revision from this batch",
				"doPutBatch() (helper run) help put",
				"doPutBatch() (helper run) next node changed (second)",
				"doPutBatch() (helper run) final version already set",
				"doPutBatch() (helper run) try simple update", 
				"doPutBatch() (helper run) try split update",
				"doPutBatch() (helper run) try merge update",
					
				"findMergeRevision() (for remove) outer loop",
				"findMergeRevision() (for remove) insertionPoint loop",
				"findMergeRevision() (for remove) found TempSplitNode",
				"findMergeRevision() (for remove) b is terminated",
				"findMergeRevision() (for remove) not regular revision help put",
				"findMergeRevision() (for remove) revision loop", "findMergeRevision() (for other) outer loop",
				"findMergeRevision() (for other) insertionPoint loop",
				"findMergeRevision() (for other) found TempSplitNode",
				"findMergeRevision() (for other) b is terminated",
				"findMergeRevision() (for other) not regular revision help put",
				"findMergeRevision() (for other) revision loop", "helpMerge() version is already set",
				"helpMergeBatch() version is already set", "helpMergeTerminator() outer loop",
				"helpMergeTerminator() outer loop, b is terminated",
				"helpMergeTerminator() outer loop, bb is null (findPredecessor())", "helpMergeTerminator() inner loop",
				"helpMergeTerminator() inner loop, bb is too far",
				"helpMergeTerminator() inner loop, bb is too far, forced restart",
				"helpMergeTerminator() inner loop, bb is terminated",
				"helpMergeTerminator() inner loop, bb is terminated, no bbb (findPredecessor())",
				"helpMergeTerminator() inner loop, found TempSplitNode, no bbb",
				"helpMergeTerminator() inner loop, found TempSplitNode, help bbb",
				"helpMergeTerminator() after inner loop, other non-regular revision help put",
				"helpMergeTerminator() after inner loop, b is terminated",
				"helpMergeTerminator() after inner loop, bb.next and b don't match",
				"helpMergeTerminator() after inner loop, bb's head is empty",
				"helpMergeTerminator() after inner loop, merge revision added (and helpped)",
				"helpMergeTerminatorBatch() outer loop", "helpMergeTerminatorBatch() outer loop, b is terminated",
				"helpMergeTerminatorBatch() outer loop, bb is null (findPredecessor())",
				"helpMergeTerminatorBatch() inner loop", "helpMergeTerminatorBatch() inner loop, bb is too far",
				"helpMergeTerminatorBatch() inner loop, bb is too far, forced restart",
				"helpMergeTerminatorBatch() inner loop, bb is terminated",
				"helpMergeTerminatorBatch() inner loop, bb is terminated, no bbb (findPredecessor())",
				"helpMergeTerminatorBatch() inner loop, found TempSplitNode, no bbb",
				"helpMergeTerminatorBatch() inner loop, found TempSplitNode, help bbb",
				"helpMergeTerminatorBatch() after inner loop, b is terminated",
				"helpMergeTerminatorBatch() after inner loop, help my merge revision",
				"helpMergeTerminatorBatch() after inner loop, other non-regular revision help put",
				"helpMergeTerminatorBatch() after inner loop, b is terminated (second check)",
				"helpMergeTerminatorBatch() after inner loop, bb.next and b don't match",
				"helpMergeTerminatorBatch() after inner loop, bb's head is empty",
				"helpMergeTerminatorBatch() after inner loop, merge revision added (and helpped)",
				"helpTempSplitNode() version already set (0 -> help put)",
				"helpTempSplitNode() version already set, full cleanup", "helpSplit() left revision",
				"helpTempSplitNodeBatch() version already set (0 -> help put)",
				"helpTempSplitNodeBatch() version already set, full cleanup", 
				
				"helpSplit() left revision",
				"helpSplit() left revision, outer loop", 
				"helpSplit() left revision, b is terminated",
				"helpSplit() left revision, version is set on b's head", 
				"helpSplit() left revision, inner loop",
				"helpSplit() left revision, inner loop, found right revision",
				"helpSplit() left revision, inner loop, version is set on left revision",
				"helpSplit() left revision, inner loop, splitNode already added",
				"helpSplit() left revision, inner loop, splitNode cassed-in", 
				"helpSplit() left revision, break from ABA",
				"helpSplit() left revision, proper node cassed-in",
				
				"helpSplitBatch() left revision",
				"helpSplitBatch() left revision, outer loop", 
				"helpSplitBatch() left revision, b is terminated",
				"helpSplitBatch() left revision, version is set on b's head", 
				"helpSplitBatch() left revision, inner loop",
				"helpSplitBatch() left revision, inner loop, found right revision",
				"helpSplitBatch() left revision, inner loop, version is set on left revision",
				"helpSplitBatch() left revision, inner loop, splitNode already added",
				"helpSplitBatch() left revision, inner loop, splitNode cassed-in", 
				"helpSplitBatch() left revision, break from ABA",
				"helpSplitBatch() left revision, proper node cassed-in",
				
				"doGc() - doGc() depth (only next or leftNext)", "doGc() - doInnerGc() depth (only next or leftNext)",
				// "Iter outer loop", "Iter skip TempSplitNode", "Iter revision found",
				"SubMapIter.ascend() outer loop", "SubMapIter.ascend() inner loop",
				"SubMapIter.ascend() inner loop, found TempSplitNode",
				"SubMapIter.ascend() inner loop, found MergeTerminator",
				"SubMapIter.ascend() revision found (should be similar to outer loop's histogram)",
				"SubMapIter.descend() outer loop", "SubMapIter.descend() findNear()", 
				
				"SubMap.forEach() outer loop (should be the same as sum of 'check of range'",
				"SubMap.forEach() check of range - will be more",
				"SubMap.forEach() check of range - won't be more, next node is null",
				"SubMap.forEach() check of range - won't be more, last key is too high",
				"SubMap.forEach() perform accept loop (if 0, we skip the contents of the revision)",
				"SubMap.forEach() inner loop",
				"SubMap.forEach() inner loop, found TempSplitNode",
				"SubMap.forEach() inner loop, found MergeTerminator",
				"SubMap.forEach() revision found (shoult be similar to outer loop's histogram)" 		
		};

		assert maps.length == descriptions.length;

		builder.append(String.format("Skips: %d\n\n", SKIP));

		for (int i = 0; i < maps.length; i++) {
			builder.append(String.format("%s:\n", descriptions[i]));
			Map<Long, Long> map = maps[i];

			List<Map.Entry<Long, Long>> list = (List<Entry<Long, Long>>) map.entrySet().stream().sorted(cmp)
					.collect(Collectors.toList());

			long total = list.stream().map(x -> x.getValue()).mapToLong(x -> x).sum();

			for (var e : list)
				builder.append(
						String.format("- %3d: %12d  %5.1f%%\n", e.getKey(), e.getValue(), 100f * e.getValue() / total));
			builder.append("\n");
		}

		return builder.toString();
	}

	public void merge(RuntimeStatistics other) {
		@SuppressWarnings("unchecked")
		Map<Long, Long>[] maps = new Map[] { getNewestRevisionLoop, getRevisionLoop, getRevisionHelpPut,
				getRevisionCreateBulkRevision, getRevisionBulkRevisionSize, findNearFindPredecessor, findNearSearchOnLowestLevel,
				findNearRevisionLoop, findNearFoundTempSplitNode, findNearFoundMergeTerminator, doGetOuterLoop,
				doGetInsertionPointLoop, doGetFoundTempSplitNode, doGetFoundMergeTerminator, doGetNextChanged,
				doPutSingleOuterLoop, doPutSingleInsertionPointLoop, doPutSingleFoundTempSplitNode,
				doPutSingleBIsTerminated, doPutSingleNotRegularHelpPut, doPutSingleNextNodeChanged,
				doPutSingleTrySimpleUpdate, doPutSingleTrySplitUpdate, doPutSingleTrySplitUpdateHelpPut,
				doRemoveSingleOuterLoop, doRemoveSingleInsertionPointLoop, doRemoveSingleFoundTempSplitNode,
				doRemoveSingleBIsTerminated, doRemoveSingleNotRegularHelpPut, doRemoveSingleNextNodeChanged,
				doRemoveSingleNothingToDo, doRemoveSingleTrySimpleUpdate, doRemoveSingleTryMergeUpdate,
				doRemoveSingleTryMergeUpdateHelpPut, 
				helpBatchPrimaryRunLoop,
				helpBatchLoop,
				
				 doPutBatchPrimaryRunOuterLoop,
					doPutBatchPrimaryRunInsertionPointLoop,
					doPutBatchPrimaryRunFoundTempSplitNode,
					doPutBatchPrimaryRunBIsTerminated,
					doPutBatchPrimaryRunNextNodeChanged,
					doPutBatchPrimaryRunFoundBatchsRevision,
					doPutBatchPrimaryRunHelpPut,
					doPutBatchPrimaryRunNextNodeChangedSecond,
					doPutBatchPrimaryRunFinalVersionSet,
					doPutBatchPrimaryRunTrySimpleUpdate,
					doPutBatchPrimaryRunTrySplitUpdate,
					doPutBatchPrimaryRunTryMergeUpdate,
				
					doPutBatchHelperRunOuterLoop,
					doPutBatchHelperRunInsertionPointLoop,
					doPutBatchHelperRunFoundTempSplitNode,
					doPutBatchHelperRunBIsTerminated,
					doPutBatchHelperRunNextNodeChanged,
					doPutBatchHelperRunFoundBatchsRevision,
					doPutBatchHelperRunHelpPut,
					doPutBatchHelperRunNextNodeChangedSecond,
					doPutBatchHelperRunFinalVersionSet,
					doPutBatchHelperRunTrySimpleUpdate,
					doPutBatchHelperRunTrySplitUpdate,
					doPutBatchHelperRunTryMergeUpdate,
				
				findMergeRevisionForRemoveOuterLoop,
				findMergeRevisionForRemoveInsertionPointLoop, findMergeRevisionForRemoveFoundTempSplitNode,
				findMergeRevisionForRemoveBIsTerminated, findMergeRevisionForRemoveHelpMergeRevision,
				findMergeRevisionForRemoveRevisionLoop, findMergeRevisionForOtherOuterLoop,
				findMergeRevisionForOtherInsertionPointLoop, findMergeRevisionForOtherFoundTempSplitNode,
				findMergeRevisionForOtherBIsTerminated, findMergeRevisionForOtherHelpMergeRevision,
				findMergeRevisionForOtherRevisionLoop, helpMergeVersionAlreadySet, helpMergeBatchVersionAlreadySet,
				helpMergeTerminatorOuterLoop, helpMergeTerminatorOuterLoopBIsTerminated,
				helpMergeTerminatorOuterLoopBBNull, helpMergeTerminatorInnerLoop, helpMergeTerminatorInnerLoopBBTooFar,
				helpMergeTerminatorInnerLoopBBTooFarForcedRestart, helpMergeTerminatorInnerLoopBBTerminated,
				helpMergeTerminatorInnerLoopBBTerminatedNoBBB, helpMergeTerminatorInnerLoopFoundTempSplitNodeNoBBB,
				helpMergeTerminatorInnerLoopFoundTempSplitNodeHelpBBB, helpMergeTerminatorOtherHelpPut,
				helpMergeTerminatorBIsTerminated, helpMergeTerminatorBBNextAndBDontMatch, helpMergeTerminatorHeadEmpty,
				helpMergeTerminatorMergeRevisionAddedAndHelp, helpMergeTerminatorBatchOuterLoop,
				helpMergeTerminatorBatchOuterLoopBIsTerminated, helpMergeTerminatorBatchOuterLoopBBNull,
				helpMergeTerminatorBatchInnerLoop, helpMergeTerminatorBatchInnerLoopBBTooFar,
				helpMergeTerminatorBatchInnerLoopBBTooFarForcedRestart, helpMergeTerminatorBatchInnerLoopBBTerminated,
				helpMergeTerminatorBatchInnerLoopBBTerminatedNoBBB,
				helpMergeTerminatorBatchInnerLoopFoundTempSplitNodeNoBBB,
				helpMergeTerminatorBatchInnerLoopFoundTempSplitNodeHelpBBB, helpMergeTerminatorBatchBIsTerminated,
				helpMergeTerminatorBatchHelpMyMerge, helpMergeTerminatorBatchOtherHelpPut,
				helpMergeTerminatorBatchBIsTerminatedSecond, helpMergeTerminatorBatchBBNextAndBDontMatch,
				helpMergeTerminatorBatchHeadEmpty, helpMergeTerminatorBatchMergeRevisionAddedAndHelp,
				helpTempSplitNodeVersionAlreadySet, helpTempSplitNodeVersionAlreadySetFullCleanup,
				helpTempSplitNodeBatchVersionAlreadySet, helpTempSplitNodeBatchVersionAlreadySetFullCleanup,
				helpSplitLeftRevision, helpSplitLeftRevisionOuterLoop,
				helpSplitLeftRevisionBIsTerminated, helpSplitLeftRevisionVersionIsSetOnBHead,
				helpSplitLeftRevisionInnerLoop, helpSplitLeftRevisionInnerLoopFoundRightRevision,
				helpSplitLeftRevisionInnerLoopVersionIsSetOnLeftRevision,
				helpSplitLeftRevisionInnerLoopSplitNodeAlreadyAdded, helpSplitLeftRevisionInnerLoopSplitNodeCassedIn,
				helpSplitLeftRevisionBreakFromABA, helpSplitLeftRevisionProperNodeCassedIn, 
				helpSplitBatchLeftRevision, helpSplitBatchLeftRevisionOuterLoop,
				helpSplitBatchLeftRevisionBIsTerminated, helpSplitBatchLeftRevisionVersionIsSetOnBHead,
				helpSplitBatchLeftRevisionInnerLoop, helpSplitBatchLeftRevisionInnerLoopFoundRightRevision,
				helpSplitBatchLeftRevisionInnerLoopVersionIsSetOnLeftRevision,
				helpSplitBatchLeftRevisionInnerLoopSplitNodeAlreadyAdded, helpSplitBatchLeftRevisionInnerLoopSplitNodeCassedIn,
				helpSplitBatchLeftRevisionBreakFromABA, helpSplitBatchLeftRevisionProperNodeCassedIn,				
				gcRegularGcDepth, gcInnerGcDepth,
				// iterAscendOuterLoop, iterAscendSkipTempSplitNode, iterAscendRevisionFound,
				subMapIterAscendOuterLoop, subMapIterAscendInnerLoop, subMapIterAscendInnerLoopFoundTempSplitNode,
				subMapIterAscendInnerLoopFoundMergeTerminator, subMapIterAscendRevisionFound,
				subMapIterDescendOuterLoop, subMapIterDescendFindNear,
				subMapForEachAscendOuterLoop,
				subMapForEachAscendWillBeMore,
				subMapForEachAscendWontBeMoreNextNull,
				subMapForEachAscendWontBeMoreTooHigh,
				subMapForEachAscendPerformAcceptLoop,
				subMapForEachAscendInnerLoop,
				subMapForEachAscendInnerLoopFoundTempSplitNode,
				subMapForEachAscendInnerLoopFoundMergeTerminator,
				subMapForEachAscendRevisionFound 		
		};

		
		@SuppressWarnings("unchecked")
		Map<Long,Long>[] otherMaps = new Map[] { other.getNewestRevisionLoop, other.getRevisionLoop, other.getRevisionHelpPut,
				other.getRevisionCreateBulkRevision, 
				other.getRevisionBulkRevisionSize,
				other.findNearFindPredecessor, other.findNearSearchOnLowestLevel,
				other.findNearRevisionLoop, other.findNearFoundTempSplitNode, other.findNearFoundMergeTerminator,
				other.doGetOuterLoop, other.doGetInsertionPointLoop, other.doGetFoundTempSplitNode,
				other.doGetFoundMergeTerminator, other.doGetNextChanged, other.doPutSingleOuterLoop,
				other.doPutSingleInsertionPointLoop, other.doPutSingleFoundTempSplitNode,
				other.doPutSingleBIsTerminated, other.doPutSingleNotRegularHelpPut, other.doPutSingleNextNodeChanged,
				other.doPutSingleTrySimpleUpdate, other.doPutSingleTrySplitUpdate,
				other.doPutSingleTrySplitUpdateHelpPut, other.doRemoveSingleOuterLoop,
				other.doRemoveSingleInsertionPointLoop, other.doRemoveSingleFoundTempSplitNode,
				other.doRemoveSingleBIsTerminated, other.doRemoveSingleNotRegularHelpPut,
				other.doRemoveSingleNextNodeChanged, other.doRemoveSingleNothingToDo,
				other.doRemoveSingleTrySimpleUpdate, other.doRemoveSingleTryMergeUpdate,
				other.doRemoveSingleTryMergeUpdateHelpPut, 
				other.helpBatchPrimaryRunLoop,
				other.helpBatchLoop,
				
				other.doPutBatchPrimaryRunOuterLoop,
				other.doPutBatchPrimaryRunInsertionPointLoop,
				other.doPutBatchPrimaryRunFoundTempSplitNode,
				other.doPutBatchPrimaryRunBIsTerminated,
				other.doPutBatchPrimaryRunNextNodeChanged,
				other.doPutBatchPrimaryRunFoundBatchsRevision,
				other.doPutBatchPrimaryRunHelpPut,
				other.doPutBatchPrimaryRunNextNodeChangedSecond,
				other.doPutBatchPrimaryRunFinalVersionSet,
				other.doPutBatchPrimaryRunTrySimpleUpdate,
				other.doPutBatchPrimaryRunTrySplitUpdate,
				other.doPutBatchPrimaryRunTryMergeUpdate,
				
				other.doPutBatchHelperRunOuterLoop,
				other.doPutBatchHelperRunInsertionPointLoop,
				other.doPutBatchHelperRunFoundTempSplitNode,
				other.doPutBatchHelperRunBIsTerminated,
				other.doPutBatchHelperRunNextNodeChanged,
				other.doPutBatchHelperRunFoundBatchsRevision,
				other.doPutBatchHelperRunHelpPut,
				other.doPutBatchHelperRunNextNodeChangedSecond,
				other.doPutBatchHelperRunFinalVersionSet,
				other.doPutBatchHelperRunTrySimpleUpdate,
				other.doPutBatchHelperRunTrySplitUpdate,
				other.doPutBatchHelperRunTryMergeUpdate,
				
				other.findMergeRevisionForRemoveOuterLoop,
				other.findMergeRevisionForRemoveInsertionPointLoop, other.findMergeRevisionForRemoveFoundTempSplitNode,
				other.findMergeRevisionForRemoveBIsTerminated, other.findMergeRevisionForRemoveHelpMergeRevision,
				other.findMergeRevisionForRemoveRevisionLoop, other.findMergeRevisionForOtherOuterLoop,
				other.findMergeRevisionForOtherInsertionPointLoop, other.findMergeRevisionForOtherFoundTempSplitNode,
				other.findMergeRevisionForOtherBIsTerminated, other.findMergeRevisionForOtherHelpMergeRevision,
				other.findMergeRevisionForOtherRevisionLoop, other.helpMergeVersionAlreadySet,
				other.helpMergeBatchVersionAlreadySet, other.helpMergeTerminatorOuterLoop,
				other.helpMergeTerminatorOuterLoopBIsTerminated, other.helpMergeTerminatorOuterLoopBBNull,
				other.helpMergeTerminatorInnerLoop, other.helpMergeTerminatorInnerLoopBBTooFar,
				other.helpMergeTerminatorInnerLoopBBTooFarForcedRestart, other.helpMergeTerminatorInnerLoopBBTerminated,
				other.helpMergeTerminatorInnerLoopBBTerminatedNoBBB,
				other.helpMergeTerminatorInnerLoopFoundTempSplitNodeNoBBB,
				other.helpMergeTerminatorInnerLoopFoundTempSplitNodeHelpBBB, other.helpMergeTerminatorOtherHelpPut,
				other.helpMergeTerminatorBIsTerminated, other.helpMergeTerminatorBBNextAndBDontMatch,
				other.helpMergeTerminatorHeadEmpty, other.helpMergeTerminatorMergeRevisionAddedAndHelp,
				other.helpMergeTerminatorBatchOuterLoop, other.helpMergeTerminatorBatchOuterLoopBIsTerminated,
				other.helpMergeTerminatorBatchOuterLoopBBNull, other.helpMergeTerminatorBatchInnerLoop,
				other.helpMergeTerminatorBatchInnerLoopBBTooFar,
				other.helpMergeTerminatorBatchInnerLoopBBTooFarForcedRestart,
				other.helpMergeTerminatorBatchInnerLoopBBTerminated,
				other.helpMergeTerminatorBatchInnerLoopBBTerminatedNoBBB,
				other.helpMergeTerminatorBatchInnerLoopFoundTempSplitNodeNoBBB,
				other.helpMergeTerminatorBatchInnerLoopFoundTempSplitNodeHelpBBB,
				other.helpMergeTerminatorBatchBIsTerminated, other.helpMergeTerminatorBatchHelpMyMerge,
				other.helpMergeTerminatorBatchOtherHelpPut, other.helpMergeTerminatorBatchBIsTerminatedSecond,
				other.helpMergeTerminatorBatchBBNextAndBDontMatch, other.helpMergeTerminatorBatchHeadEmpty,
				other.helpMergeTerminatorBatchMergeRevisionAddedAndHelp, other.helpTempSplitNodeVersionAlreadySet,
				other.helpTempSplitNodeVersionAlreadySetFullCleanup, other.helpTempSplitNodeBatchVersionAlreadySet,
				other.helpTempSplitNodeBatchVersionAlreadySetFullCleanup, other.helpSplitLeftRevision,
				other.helpSplitLeftRevision, other.helpSplitLeftRevisionOuterLoop,
				other.helpSplitLeftRevisionBIsTerminated, other.helpSplitLeftRevisionVersionIsSetOnBHead,
				other.helpSplitLeftRevisionInnerLoop, other.helpSplitLeftRevisionInnerLoopFoundRightRevision,
				other.helpSplitLeftRevisionInnerLoopVersionIsSetOnLeftRevision,
				other.helpSplitLeftRevisionInnerLoopSplitNodeAlreadyAdded, other.helpSplitLeftRevisionInnerLoopSplitNodeCassedIn,
				other.helpSplitLeftRevisionBreakFromABA, other.helpSplitLeftRevisionProperNodeCassedIn,
				other.helpSplitBatchLeftRevision, other.helpSplitBatchLeftRevisionOuterLoop,
				other.helpSplitBatchLeftRevisionBIsTerminated, other.helpSplitBatchLeftRevisionVersionIsSetOnBHead,
				other.helpSplitBatchLeftRevisionInnerLoop, other.helpSplitBatchLeftRevisionInnerLoopFoundRightRevision,
				other.helpSplitBatchLeftRevisionInnerLoopVersionIsSetOnLeftRevision,
				other.helpSplitBatchLeftRevisionInnerLoopSplitNodeAlreadyAdded, other.helpSplitBatchLeftRevisionInnerLoopSplitNodeCassedIn,
				other.helpSplitBatchLeftRevisionBreakFromABA, other.helpSplitBatchLeftRevisionProperNodeCassedIn,
			
				other.gcRegularGcDepth, other.gcInnerGcDepth,
				// other.iterAscendOuterLoop, other.iterAscendSkipTempSplitNode,
				// other.iterAscendRevisionFound,
				other.subMapIterAscendOuterLoop, other.subMapIterAscendInnerLoop,
				other.subMapIterAscendInnerLoopFoundTempSplitNode, other.subMapIterAscendInnerLoopFoundMergeTerminator,
				other.subMapIterAscendRevisionFound, other.subMapIterDescendOuterLoop,
				other.subMapIterDescendFindNear,
				
				other.subMapForEachAscendOuterLoop,
				other.subMapForEachAscendWillBeMore,
				other.subMapForEachAscendWontBeMoreNextNull,
				other.subMapForEachAscendWontBeMoreTooHigh,
				other.subMapForEachAscendPerformAcceptLoop,
				other.subMapForEachAscendInnerLoop,
				other.subMapForEachAscendInnerLoopFoundTempSplitNode,
				other.subMapForEachAscendInnerLoopFoundMergeTerminator,
				other.subMapForEachAscendRevisionFound };

		assert maps.length == otherMaps.length;

		for (int i = 0; i < maps.length; i++) {
			Map<Long, Long> map = (Map<Long, Long>) maps[i];
			Map<Long, Long> otherMap = (Map<Long, Long>) otherMaps[i];

			for (var e : otherMap.entrySet()) {
				Long count = map.get(e.getKey());
				if (count == null)
					map.put(e.getKey(), e.getValue());
				else
					map.put(e.getKey(), count + e.getValue());

			}
		}
	}
}
