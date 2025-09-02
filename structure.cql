create keyspace if not exists {meta_keyspace} WITH replication = \{
	'class': 'NetworkTopologyStrategy',
	'replication_factor' : {replication_factor}
} AND TABLETS = \{'enabled': false};

create table if not exists {meta_keyspace}.app (
	name text,
	instance uuid,
	"keyspace" text,
	implementation text,
	versions list<tuple<text, timestamp>>,
	primary key (instance, name)
);

create table if not exists {meta_keyspace}.migration (
	app_instance uuid,
	app_name text,
	file_name text,
	"index" int,
	created timestamp,
	primary key (app_instance, app_name, file_name)
);

create keyspace if not exists "{data_keyspace}" WITH replication = \{
	'class': 'NetworkTopologyStrategy',
	'replication_factor' : {replication_factor}
} AND TABLETS = \{'enabled': false};
