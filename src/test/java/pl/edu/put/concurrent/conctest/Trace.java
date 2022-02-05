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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Data;
import lombok.Value;
import pl.edu.put.utils.Pair;

@Data
public class Trace {

	private static final int INIT_SIZE = 1000000;

	enum EventType {
		Timestamp("Ts"), Get("Get"), Put("Put"), Delete("Del"), Batch("Bat"), Snapshot("Snp");

		String desc;

		EventType(String desc) {
			this.desc = desc;
		}

		@Override
		public String toString() {
			return desc;
		}
	}

	public abstract static class Event {
		EventType type;
		int threadId;
		int seq;
		
		public abstract boolean isUpdating();
		public abstract Pair<Integer, String>[] getWitnessValues();
		public abstract Pair<Integer, String>[] getWrittenValues();
		public abstract int[] getUpdatedKeys();
		
		public String getSignature() {
			return String.format("%d-%d",  threadId, seq);
		}
		
		public String toString() {
			return String.format("%d-%d %3s",  threadId, seq, type);
		}
		
		@Override
		public boolean equals(Object o){
		  if ( !(o instanceof Event)){
		    return false;
		  }
		  Event t = (Event)o;
		  return type.equals(t.type) && threadId == t.threadId && seq == t.seq;
		}
	}

	@Data
	public static class TimestampEvent extends Event {
		long timestamp;

		public TimestampEvent(long timestamp) {
			this.type = EventType.Timestamp;
			this.timestamp = timestamp;
		}

		@Override
		public String toString() {
			return String.format("%s %d", super.toString(), timestamp);
		}

		@Override
		public boolean isUpdating() {
			return false;
		}

		@Override
		public Pair<Integer, String>[] getWitnessValues() {
			return null;
		}

		@Override
		public Pair<Integer, String>[] getWrittenValues() {
			return null;
		}

		@Override
		public int[] getUpdatedKeys() {
			return null;
		}
	}

	@Data
	public static class GetEvent extends Event {
		int key;
		String value;

		public GetEvent(int key, String value) {
			this.type = EventType.Get;
			this.key = key;
			this.value = value;
		}

		@Override
		public String toString() {
			return String.format("%s %s %s", super.toString(), key, value);
		}
		
		public String toStringShort() {
			return String.format("%s %s %s", type, key, value);
		}

		@Override
		public boolean isUpdating() {
			return false;
		}

		@SuppressWarnings("unchecked")
		@Override
		public Pair<Integer, String>[] getWitnessValues() {
			return new Pair[] { Pair.makePair(key, value) };
		}

		@Override
		public Pair<Integer, String>[] getWrittenValues() {
			return null;
		}

		@Override
		public int[] getUpdatedKeys() {
			return null;
		}
	}

	@Data
	public static class PutEvent extends Event {
		int key;
		String value;
		String oldValue;

		public PutEvent(int key, String value, String oldValue) {
			this.type = EventType.Put;
			this.key = key;
			this.value = value;
			this.oldValue = oldValue;
		}

		@Override
		public String toString() {
			return String.format("%s %s %s (%s)", super.toString(), key, value, oldValue != null ? oldValue : "null");
		}
		
		public String toStringShort() {
			return String.format("%s %s %s (%s)", type, key, value, oldValue != null ? oldValue : "null");
		}

		@Override
		public boolean isUpdating() {
			return true;
		}

		@SuppressWarnings("unchecked")
		@Override
		public Pair<Integer, String>[] getWitnessValues() {
			return new Pair[] { Pair.makePair(key, oldValue) };
		}

		@SuppressWarnings("unchecked")
		@Override
		public Pair<Integer, String>[] getWrittenValues() {
			return new Pair[] { Pair.makePair(key, value) };
		}

		@Override
		public int[] getUpdatedKeys() {
			return new int[] { key };
		}
	}

	@Data
	public static class DeleteEvent extends Event {
		int key;
		String oldValue;

		public DeleteEvent(int key, String oldValue) {
			this.type = EventType.Delete;
			this.key = key;
			this.oldValue = oldValue;
		}

		@Override
		public String toString() {
			return String.format("%s %s (%s)", super.toString(), key, oldValue != null ? oldValue : "null");
		}
		
		public String toStringShort() {
			return String.format("%s %s (%s)", type, key, oldValue != null ? oldValue : "null");
		}

		@Override
		public boolean isUpdating() {
			return true;
		}

		@SuppressWarnings("unchecked")
		@Override
		public Pair<Integer, String>[] getWitnessValues() {
			return new Pair[] { Pair.makePair(key, oldValue) };
		}

		@Override
		public Pair<Integer, String>[] getWrittenValues() {
			return null;
		}

		@Override
		public int[] getUpdatedKeys() {
			return new int[] { key };
		}
	}

	@Data
	public static class BatchEvent extends Event {
		Map<Integer, PutEvent> putEvents;
		Map<Integer, DeleteEvent> deleteEvents;

		public BatchEvent() {
			this.type = EventType.Batch;
			putEvents = new HashMap<>();
			deleteEvents = new HashMap<>();
		}

		public void put(int key, String value) {
			putEvents.put(key, new PutEvent(key, value, null));
		}

		public void delete(int key) {
			deleteEvents.put(key, new DeleteEvent(key, null));
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();

			builder.append(super.toString() + " [");
			for (var e : putEvents.values())
				builder.append(e.toStringShort() + ", ");
			for (var e : deleteEvents.values())
				builder.append(e.toStringShort() + ", ");
			builder.replace(builder.length() - 2, builder.length(), "]");

			return builder.toString();
		}

		public void setOldVersion(Integer key, String oldValue) {
			PutEvent pe = putEvents.get(key);
			if (pe != null) {
				pe.oldValue = oldValue;
			} else {
				deleteEvents.get(key).oldValue = oldValue;
			}
		}

		@Override
		public boolean isUpdating() {
			return true;
		}

		@SuppressWarnings("unchecked")
		@Override
		public Pair<Integer, String>[] getWitnessValues() {
			Pair<Integer, String>[] witnesses = new Pair[putEvents.size() + deleteEvents.size()];
			int i = 0;
			for (var e : putEvents.entrySet()) {
				int key =  e.getKey();
				String s = e.getValue().oldValue;
				witnesses[i++] = Pair.makePair(key, s);
			}
			for (var e : deleteEvents.entrySet()) {
				int key = e.getKey();
				String s = e.getValue().oldValue;
				witnesses[i++] = Pair.makePair(key, s);
			}
			return witnesses;
		}

		@SuppressWarnings("unchecked")
		@Override
		public Pair<Integer, String>[] getWrittenValues() {
			Pair<Integer, String>[] writtenValues = new Pair[putEvents.size()];
			int i = 0;
			for (var e : putEvents.entrySet()) {
				writtenValues[i++] = Pair.makePair(e.getKey(), e.getValue().value);
			}
			return writtenValues;
		}

		@Override
		public int[] getUpdatedKeys() {
			int[] keys = new int[putEvents.size() + deleteEvents.size()];
			int i = 0;
			for (var e : putEvents.keySet())
				keys[i++] = e;
			for (var e : deleteEvents.keySet())
				keys[i++] = e;
			return keys;
		}
	}

	@Data
	public static class SnapshotEvent extends Event {
		List<GetEvent> getEvents = new ArrayList<>();

		public SnapshotEvent() {
			type = EventType.Snapshot;
		}

		public void append(int key, String value) {
			getEvents.add(new GetEvent(key, value));
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();

			builder.append(super.toString() + " [");
			for (var e : getEvents)
				builder.append(e.toStringShort() + ", ");
			if (getEvents.size() > 1)
				builder.replace(builder.length() - 2, builder.length(), "]");
			else 
				builder.append("]");

			return builder.toString();
		}

		@Override
		public boolean isUpdating() {
			return false;
		}

		@SuppressWarnings("unchecked")
		@Override
		public Pair<Integer, String>[] getWitnessValues() {
			Pair<Integer, String>[] witnesses = new Pair[getEvents.size()];
			int i = 0;
			for (var e : getEvents) {
				int key = e.key;
				String s = e.value;
				witnesses[i++] = Pair.makePair(key, s);
			}
			return witnesses;
		}

		@Override
		public Pair<Integer, String>[] getWrittenValues() {
			return null;
		}

		@Override
		public int[] getUpdatedKeys() {
			return null;
		}
	}

	List<Event> events = new ArrayList<>(INIT_SIZE);
	int threadId; 
	int seq = 0;
	
	public Trace(int threadId) {
		this.threadId = threadId;
	}

	public void append(Event e) {
		e.threadId = threadId;
		e.seq = seq++;
		events.add(e);
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		for (var e : events)
			builder.append(String.format("%s\n", e));
		return builder.toString();
	}
}
