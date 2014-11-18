package com.datastax.queue.dao;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.model.Job;

public class QueueDao {

	private static final String DUMMY_KEY = "Counters";
	private static Logger logger = LoggerFactory.getLogger( QueueDao.class );	
	private Session session;
	
	private DateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd");
	private static String keyspaceName = "datastax_queueing_demo";
	private static String queueTable = keyspaceName + ".queue";
	private static String queueCountersTable = keyspaceName + ".queue_counters";

	private String INSERT_INTO_QUEUE = "insert into " + queueTable + " (location, job, task) values (?,?,?)";
	private String READ_QUEUE = "select * from " + queueTable + " where location = ?";
	private String INSERT_INTO_QUEUE_READER_COUNTER = "insert into " + queueCountersTable + " (dummy, reader) values (?,?)";
	private String INSERT_INTO_QUEUE_WRITER_COUNTER = "insert into " + queueCountersTable + " (dummy, writer) values (?,?)";
	
	private String READ_QUEUE_COUNTERS = "select reader,writer from " + queueCountersTable + " where dummy = '" + DUMMY_KEY + "'";
	private String READ_QUEUE_READER = "select reader from " + queueCountersTable + " where dummy = '" + DUMMY_KEY + "'";
	private String READ_QUEUE_WRITER = "select writer from " + queueCountersTable + " where dummy = '" + DUMMY_KEY + "'";
	
	
	private PreparedStatement queueInsert;
	private PreparedStatement queueRead;
	private PreparedStatement queueCounterInsertReader;
	private PreparedStatement queueCounterInsertWriter;
	private PreparedStatement queueCounterReadReader;
	private PreparedStatement queueCounterReadWriter;
	private PreparedStatement queueCounterRead;
	
	public QueueDao(String[] contactPoints) {

		Cluster cluster = Cluster.builder()				
				.addContactPoints(contactPoints)
				.build();
		
		this.session = cluster.connect();

		this.queueInsert = session.prepare(INSERT_INTO_QUEUE);
		this.queueRead = session.prepare(READ_QUEUE);		
		this.queueCounterInsertReader = session.prepare(INSERT_INTO_QUEUE_READER_COUNTER);		
		this.queueCounterInsertWriter = session.prepare(INSERT_INTO_QUEUE_WRITER_COUNTER);
		this.queueCounterReadReader = session.prepare(READ_QUEUE_READER);
		this.queueCounterReadWriter = session.prepare(READ_QUEUE_WRITER);
		this.queueCounterRead = session.prepare(READ_QUEUE_COUNTERS);
		
		this.queueCounterInsertReader.setConsistencyLevel(ConsistencyLevel.QUORUM);
		this.queueCounterInsertWriter.setConsistencyLevel(ConsistencyLevel.QUORUM);
		this.queueCounterReadReader.setConsistencyLevel(ConsistencyLevel.QUORUM);
		this.queueCounterReadWriter.setConsistencyLevel(ConsistencyLevel.QUORUM);
		this.queueCounterRead.setConsistencyLevel(ConsistencyLevel.QUORUM);
		
		this.queueInsert.setConsistencyLevel(ConsistencyLevel.ALL);
		this.queueRead.setConsistencyLevel(ConsistencyLevel.ONE);
	}
	
	public void insertJob (int location, UUID job, String task){
				
		session.execute(this.queueInsert.bind(location, job, task));
	}

	public void updateReaderCount(int count){
		
		session.execute(this.queueCounterInsertReader.bind(DUMMY_KEY, count));
	}
	
	public void updateWriterCount(int count){
		
		session.execute(this.queueCounterInsertWriter.bind(DUMMY_KEY, count));
	}

	public QueueCounts getQueueCounts() {
		
		ResultSet result = session.execute(this.queueCounterRead.bind());

		if (result.isExhausted()){
			return new QueueCounts(0,0);
		}
		Row row = result.one();
		int reader = row.getInt("reader");
		int writer = row.getInt("writer");
		
		return new QueueCounts(reader,writer);
	}

	public Job getJobFromQueue(int location) {
		
		ResultSet result = session.execute(this.queueRead.bind(location));

		if (result.isExhausted()){
			throw new RuntimeException("No Job for location " + location + " found");
		}

		Row row = result.one();
		
		UUID job = row.getUUID("job");
		String task = row.getString("task");
		
		return new Job(job, task);
	}
}
