package com.datastax.queueing;

import com.datastax.model.Job;
import com.datastax.queue.dao.QueueCounts;
import com.datastax.queue.dao.QueueDao;

public class CassandraQueue {

	private QueueDao dao;
	private QueueCounts queueCounts;
	private int jobsSize;
	private int maxQueueSize;
	
	public CassandraQueue(String[] contactPoints, int maxQueueSize, int jobsSize) {

		this.maxQueueSize = maxQueueSize;
		this.jobsSize = jobsSize;
		this.dao = new QueueDao(contactPoints);
		
		this.queueCounts = dao.getQueueCounts();
	}

	/**
	 * Get a Job from the queue
	 * @return Job
	 */
	public Job poll() {
		
		this.queueCounts = dao.getQueueCounts();
		
		if (queueCounts.difference() > 0){
			
			Job job = dao.getJobFromQueue(queueCounts.getReaderCount() % maxQueueSize);			
			return job;
		}
		return null;
	}

	public int getReaderLocation(){
		return queueCounts.getReaderCount() % maxQueueSize;
	}
	
	public int getWriterLocation(){
		return queueCounts.getWriterCount() % maxQueueSize;
	}
	
	public boolean offer(Job job) {
		this.queueCounts = dao.getQueueCounts();
		
		if (queueCounts.difference() > jobsSize){
			return false;
		}
		dao.insertJob(queueCounts.getWriterCount() % maxQueueSize, job.getId(), job.getTask());					;
		return true;
	}

	public void removeWritten() {
		dao.updateWriterCount(queueCounts.getWriterCount() + 1);		
	}

	
	public void removeRead() {
		dao.updateReaderCount(queueCounts.getReaderCount() + 1);		
	}
}
