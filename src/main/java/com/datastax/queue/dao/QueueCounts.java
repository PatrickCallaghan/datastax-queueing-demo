package com.datastax.queue.dao;

import com.datastax.model.Job;

public class QueueCounts {
	private int readerCount = 0;
	private int writerCount = 0;

	public QueueCounts(int readerCount, int writerCount) {
		super();
		this.readerCount = readerCount;
		this.writerCount = writerCount;
	}

	public int getReaderCount() {
		return readerCount;
	}

	public int getWriterCount() {
		return writerCount;
	}
	
	public int difference(){
		return writerCount - readerCount;
	}

	@Override
	public String toString() {
		return "QueueCounts [readerCount=" + readerCount + ", writerCount=" + writerCount + "]";
	}
}
