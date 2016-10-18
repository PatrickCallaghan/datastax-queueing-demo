create keyspace if not exists datastax_queueing_demo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1' };

use datastax_queueing_demo; 
 
create table if not exists queue (location int, job uuid, task text, PRIMARY KEY (location)) WITH compaction= { 'class': 'MemoryOnlyStrategy', 'size_limit_in_mb': 100 }  AND caching = {'keys':'NONE', 'rows_per_partition':'NONE'};
	
create table if not exists queue_counters (dummy text, reader int, writer int, PRIMARY KEY (dummy))WITH compaction= { 'class': 'MemoryOnlyStrategy', 'size_limit_in_mb': 1 }  AND caching = {'keys':'NONE', 'rows_per_partition':'NONE'};
	
insert into queue_counters (dummy, reader, writer) values ('Counters', 0, 0);

truncate queue;
truncate queue_counters;

