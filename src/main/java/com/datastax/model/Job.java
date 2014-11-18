package com.datastax.model;

import java.util.UUID;

public class Job {

	private UUID id;
	private String task;

	public Job(UUID id, String task) {
		super();
		this.id = id;
		this.task = task;
	}

	public UUID getId() {
		return id;
	}

	public String getTask() {
		return task;
	}

	@Override
	public String toString() {
		return "Job [id=" + id + ", task=" + task + "]";
	}
}
