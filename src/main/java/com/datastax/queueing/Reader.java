package com.datastax.queueing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.datastax.model.Job;

public class Reader {

	private static Logger logger = LoggerFactory.getLogger(Reader.class);

	public Reader() {

		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		String maxJobSizeStr = PropertyHelper.getProperty("maxJobSize", "500");
		String maxQueueSizeStr = PropertyHelper.getProperty("maxQueueSize", "1000");
		
		int maxQueueSize = Integer.parseInt(maxQueueSizeStr);
		int maxJobSize = Integer.parseInt(maxJobSizeStr);
		
		CassandraQueue queue = new CassandraQueue(contactPointsStr.split(","), maxQueueSize, maxJobSize);
		
		Timer timer = new Timer();
		logger.info("Starting Queue Reader");
			
		boolean finished = true;
		
		while (finished){
			
			//Read Counter
			Job job = queue.poll();
			if (job != null){
				logger.info("Processing Job " + job.getId() + " from location " + queue.getReaderLocation());
				queue.removeRead();
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