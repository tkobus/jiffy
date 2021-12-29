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

import org.jgrapht.graph.DefaultEdge;

public class LabeledEdge extends DefaultEdge {

	public enum LabelType {
		ProgramOrder("PO"), Timestamp("TS"), Witness("WT"), Overwritten("OW"), InitTimestampRefinment("ITSR"),
		TimestampRefinement("TSR"), FollowRefinement("FR"), FollowDeleteRefinement("FDR"), DeleteRefinement("DR");

		String s;

		LabelType(String s) {
			this.s = s;
		}

		public String toString() {
			return s;
		}
	}

	private static final long serialVersionUID = 2721712436167625647L;
	LabelType label;

	public LabeledEdge(LabelType label) {
		super();
		this.label = label;
	}

	public LabelType getLabel() {
		return label;
	}

	public String toStringShort() {
		return String.format("(%s)", label);
	}
	
	@Override
	public String toString() {
		return String.format("(%s : %s : %s)", getSource(), getTarget(), label);
	}
}
