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

package pl.edu.put.utils;

public class Quad<F, S, T, L> {

	public static <P, Q, R, S> Quad<P, Q, R, S> makeTriple(P p, Q q, R r, S s) {
		return new Quad<P, Q, R, S>(p, q, r, s);
	}

	public final F first;
	public final S second;
	public final T third;
	public final L fourth;

	public Quad(F first, S second, T third, L fourth) {
		this.first = first;
		this.second = second;
		this.third = third;
		this.fourth = fourth;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((first == null) ? 0 : first.hashCode());
		result = prime * result + ((second == null) ? 0 : second.hashCode());
		result = prime * result + ((third == null) ? 0 : third.hashCode());
		result = prime * result + ((fourth == null) ? 0 : fourth.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		@SuppressWarnings("rawtypes")
		Quad other = (Quad) obj;
		if (first == null) {
			if (other.first != null) {
				return false;
			}
		} else if (!first.equals(other.first)) {
			return false;
		}
		if (second == null) {
			if (other.second != null) {
				return false;
			}
		} else if (!second.equals(other.second)) {
			return false;
		}
		if (third == null) {
			if (other.third != null) {
				return false;
			}
		} else if (!third.equals(other.third)) {
			return false;
		}
		if (fourth == null) {
			if (other.fourth != null) {
				return false;
			}
		} else if (!fourth.equals(other.fourth)) {
			return false;
		}
		return true;
	}

	public boolean isInstance(Class<?> classA, Class<?> classB, Class<?> classC, Class<?> classD) {
		return classA.isInstance(first) && classB.isInstance(second) && classC.isInstance(third)
				&& classD.isInstance(fourth);
	}

	@SuppressWarnings("unchecked")
	public static <P, Q, R, S> Quad<P, Q, R, S> cast(Quad<?, ?, ?, ?> quad, Class<P> pClass, Class<Q> qClass,
			Class<R> rClass, Class<S> sClass) {

		if (quad.isInstance(pClass, qClass, rClass, sClass)) {
			return (Quad<P, Q, R, S>) quad;
		}

		throw new ClassCastException();

	}

}