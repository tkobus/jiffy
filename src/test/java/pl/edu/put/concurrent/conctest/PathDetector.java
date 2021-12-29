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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.jgrapht.Graph;

public class PathDetector {

	public static class CycleFoundException extends CheckerException {
		private static final long serialVersionUID = -7178999731297836442L;
		Stack<Node> stack;

		public CycleFoundException(Stack<Node> stack) {
			this.stack = stack;
		}

		public String getCycle() {
			StringBuilder builder = new StringBuilder();
			builder.append("cycle: \n");
			Iterator<Node> iter = stack.iterator();
			while (iter.hasNext()) {
				builder.append(iter.next() + "\n");
			}
			return builder.toString();
		}
	}

	enum Color {
		Gray, Black;
	}

	Graph<Node, LabeledEdge> graph;
	Node source;
	Map<Node, Color> nodesColors;
	Stack<Node> stack;
	Set<Node> reachableNodes;
	

	public PathDetector(Graph<Node, LabeledEdge> graph, Node source) {
		this.graph = graph;
		this.source = source;
		this.reachableNodes = new HashSet<>();
		reset();
	}

	public void reset() {
		this.nodesColors = new HashMap<>();
		this.stack = new Stack<>();
	}

	public boolean pathExistsTo(Node target) throws CycleFoundException {
		if (source.maxTimestamp <= target.minTimestamp)
			return true;
		
		if (reachableNodes.contains(target))
			return true;
		
		reset();
		stack.push(source);

		while (!stack.isEmpty()) {
			Node currentNode = stack.peek();
			Color currentColor = nodesColors.get(currentNode);
			if (currentColor == null) {
				nodesColors.put(currentNode, Color.Gray);
				reachableNodes.add(currentNode);
				Set<LabeledEdge> outgoingEdges = graph.outgoingEdgesOf(currentNode);
				for (LabeledEdge e : outgoingEdges) {
					Node neighbour = graph.getEdgeTarget(e);
					reachableNodes.add(neighbour);
					if (neighbour.equals(target)) 
						return true;
					Color neighbourColor = nodesColors.get(neighbour);
					if (Color.Gray.equals(neighbourColor))
						throw new CycleFoundException(stack);
					if (neighbourColor == null && !(target.maxTimestamp <= neighbour.minTimestamp))
						stack.push(neighbour);
				}
			} else {
				nodesColors.put(currentNode, Color.Black);
				stack.pop();
			}
		}

		return false;
	}
}
