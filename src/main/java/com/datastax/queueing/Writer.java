package com.datastax.queueing;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.datastax.model.Job;

public class Writer {

	private static Logger logger = LoggerFactory.getLogger(Writer.class);

	public Writer() {

		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		String maxQueueSizeStr = PropertyHelper.getProperty("maxQueueSize", "1000");
		String maxJobSizeStr = PropertyHelper.getProperty("maxJobSize", "500");
		String jobsStr = PropertyHelper.getProperty("jobs", "1000000000");

		int maxQueueSize = Integer.parseInt(maxQueueSizeStr);
		int maxJobSize = Integer.parseInt(maxJobSizeStr);
		int jobsSize = Integer.parseInt(jobsStr);

		Timer timer = new Timer();
		logger.info("Starting Queue Writer with maxQueueSize: " + maxQueueSize + " and maxJobSize: " + maxJobSize);

		CassandraQueue queue = new CassandraQueue(contactPointsStr.split(","), maxQueueSize, maxJobSize);

		for (int i = 0; i < jobsSize; i++) {

			boolean space = true;

			while (space == true) {

				Job job = new Job(UUID.randomUUID(), issuers.get(new Double(Math.random() * issuers.size()).intValue())
						+ "-" + System.currentTimeMillis());

				// Block if job can't be added
				while (!queue.offer(job)) {
					sleep(1);
				}

				logger.info("Job " + job.getId() + " added in location " + queue.getWriterLocation());

				queue.removeWritten();
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
		new Writer();

		System.exit(0);
	}

	private List<String> locations = Arrays.asList("London", "Manchester", "Liverpool", "Glasgow", "Dundee",
			"Birmingham");

	private List<String> issuers = Arrays.asList("Tesco", "Sainsbury", "Asda Wal-Mart Stores", "Morrisons",
			"Marks & Spencer", "Boots", "John Lewis", "Waitrose", "Argos", "Co-op", "Currys", "PC World", "B&Q",
			"Somerfield", "Next", "Spar", "Amazon", "Costa", "Starbucks", "BestBuy", "Wickes", "TFL", "National Rail",
			"Pizza Hut", "Local Pub");
}
