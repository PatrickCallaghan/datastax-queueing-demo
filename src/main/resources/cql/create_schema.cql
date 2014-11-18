create keyspace if not exists datastax_queueing_demo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1' };

use datastax_queueing_demo; 
 
create table queue (location int, job uuid, task text, PRIMARY KEY (location)) WITH compaction= { 'class': 'MemoryOnlyStrategy', 'size_limit_in_mb': 100 } AND caching = 'NONE';
	
create table queue_counters (dummy text, reader int, writer int, PRIMARY KEY (dummy))WITH compaction= { 'class': 'MemoryOnlyStrategy', 'size_limit_in_mb': 1 } AND caching ='NONE';
	
truncate queue;
truncate queue_counters;

