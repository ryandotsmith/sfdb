#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <error.h>
#include <db.h>

int
main()
{
	DB_ENV *env;
	DB *db;

	int ret;
	if ((ret = init_env(&env)) != 0) {
		fprintf(stderr, "init_env error: %s\n", db_strerror(ret));
		exit(1);
	}
	printf("at=env-initialized\n");
	if ((ret = init_db(&db, env)) != 0) {
		fprintf(stderr, "init_db error: %s\n", db_strerror(ret));
		exit(1);
	}
	printf("at=db-initialized\n");

	char *payload = "{\"hello\": \"world\"}";
	enqueue(db, payload);
	walk(db);
	return 0;
}

int 
init_env(DB_ENV **dbenvp)
{
	DB_ENV *dbenv;
	int ret;
	if (ret = db_env_create(&dbenv, 0) != 0) {
		return ret;
	}
	ret = dbenv->open(dbenv, 
		"/tmp/kq-env", 
		DB_CREATE | 
		DB_RECOVER | 
		DB_INIT_LOCK | 
		DB_INIT_LOG | 
		DB_INIT_TXN | 
		DB_INIT_MPOOL,
		0);
	if (ret != 0) {
		return ret;
	}
	*dbenvp = dbenv;
	return 0;
}

int
init_db(DB **dbp, DB_ENV *env)
{
	DB *db;
	int ret;
	if (ret = db_create(&db, env, 0) != 0) {
		return ret;
	}
	if ((ret = db->set_re_len(db, 128)) != 0) {
		return ret;
	}
	u_int32_t db_flags = DB_AUTO_COMMIT | DB_READ_UNCOMMITTED;
	ret = db->open(db, NULL, "kq.db", NULL, DB_UNKNOWN, db_flags, 0);
	if (ret != 0) {
		//If the error is ENOENT we should create the database.
		if (ret == 2) {
			printf("at=create-database\n");
			ret = db->open(db, 
					NULL, 
					"kq.db", 
					NULL, 
					DB_QUEUE, 
					db_flags | DB_CREATE, 
					0);
		}
		if (ret != 0) {
			return ret;
		}
	}
	*dbp = db;
	return 0;
}

int
walk(DB *db)
{
	DBT k, v;
	memset(&k, 0, sizeof(DBT));
	memset(&v, 0, sizeof(DBT));
	int ret;
	DBC *dbcp;
	ret = db->cursor(db, NULL, &dbcp, 0);
	if (ret != 0) {
		return ret;
	}
	while (ret = dbcp->get(dbcp, &k, &v, DB_NEXT) == 0) {
		printf("k=%d v=%s\n", *(char *)k.data, (char *)v.data);
	}
	return 0;
}

int
enqueue(DB *db, void *data)
{
	DBT k, v;
	memset(&k, 0, sizeof(DBT));
	memset(&v, 0, sizeof(DBT));
	
	v.data = data;
	v.size = strlen(data) + 1;

	int ret;
	ret = db->put(db, NULL, &k, &v, DB_APPEND);
	if (ret != 0) {
		fprintf(stderr, "db_put: %s\n", db_strerror(ret));
	}
	printf("at=put key=%d\n", *(char *)k.data);
	return 0;
}
