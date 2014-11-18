package com.datastax.queueing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.datastax.model.Job;
import com.datastax.queue.dao.QueueCounts;
import com.datastax.queue.dao.QueueDao;

public class Reader {

	private static Logger logger = LoggerFactory.getLogger(Reader.class);

	public Reader() {

		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		String jobsStr = PropertyHelper.getProperty("jobs", "1000000");
		String maxQueueSizeStr = PropertyHelper.getProperty("maxQueueSize", "1000");

		QueueDao dao = new QueueDao(contactPointsStr.split(","));
		int maxQueueSize = Integer.parseInt(maxQueueSizeStr);
		int jobsSize = Integer.parseInt(jobsStr);
		
		Timer timer = new Timer();
		logger.info("Starting Queue Reader");
			
		boolean finished = true;
		
		while (finished){
			
			//Read Counter
			QueueCounts queueCounts = dao.getQueueCounts();
			
			if (queueCounts.difference() > 0){
				//Read Test Job
				Job job = dao.getJobFromQueue(queueCounts.getReaderCount() % maxQueueSize);				
				logger.info("Processing Job " + job.getId() + " from location " + queueCounts.getReaderCount() % maxQueueSize);
				
				//Process the job.
				//sleep(10);
				dao.updateReaderCount(queueCounts.getReaderCount() + 1);
			}else{			
				//If nothing in queue, sleep
				logger.info("Waiting");
				sleep(50);
			}
			
			if (queueCounts.getReaderCount()==jobsSize){
				finished = true;
			}
		}
		timer.end();
	}

	private void sleep(int i) {
		try {
			Thread.sleep(i);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new Reader();
		System.exit(0);
	}
}