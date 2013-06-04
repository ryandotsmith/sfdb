#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <error.h>
#include <signal.h>
#include <task.h>
#include <db.h>

enum
{
        STACK = 32768
};

typedef struct conn {
	DB_ENV *env;
	DB *db;
	int fd;
} conn;

void handle_req(void *);

void
taskmain(int argc , char **argv)
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

	int cfd, fd;
	int rport;
	char remote[16];

	fd = netannounce(TCP, 0, 8000);
	if (fd < 0) {
		fprintf(stderr, "Unable to announce on port 8000.\n");
		exit(1);
	}
	fdnoblock(fd);
	while((cfd = netaccept(fd, remote, &rport)) >= 0) {
		conn *c = malloc(sizeof(conn));
		c->db = db;
		c->env = env;
		c->fd = cfd;
		taskcreate(handle_req, (void *)c, STACK);
	}
	return;
}

//Read from fd until we find c.
//Bytes up till c will be stored in buff.
int
readuntil(int fd, char c, char *buff)
{
	int n, found, nread;
	nread = 0;
	found = 0;

	while(found == 0) {
		if ((n = fdread(fd, buff + nread, 1)) < 1) {
			return n;
		}
		if (buff[nread] == c) {
			found = 1;
		}
		nread++;
	}
	return nread;
}

int
readnbytes(int fd, int nbytes, char *buff)
{
	int n, nread;
	nread = 0;
	n = 0;
	while(nread < nbytes) {
		if ((n = fdread(fd, buff + nread, nbytes - nread)) < 1) {
			return n;
		}
		nread += n;
	}
	return nread;
}

void
handle_req(void *v)
{
	int ret;
	conn *c;
	c = (conn *)v;

	char lens[4];
	memset(&lens, 0, 4);
	int n;
	n = readuntil(c->fd, ' ', lens);
	int len;
	len = atoi(lens);

	char buff[len + 1];
	memset(buff, 0, len + 1);
	readnbytes(c->fd, len, buff);

	char body[len - 2];
	strncpy(body, buff+2, len - 2);

	char *resp;
	if (buff[0] == 'p') {
		if ((ret = put(c->db, body, &resp)) <= 0) {
			ret = build_error("e unable to put", &resp);
			fprintf(stderr, "error=%s\n", db_strerror(ret));
		}
	} else if (buff[0] == 'g') {
		if ((ret = get(c->db, body, &resp)) <= 0) {
			ret = build_error("e unable to put", &resp);
			fprintf(stderr, "error=%s\n", db_strerror(ret));
		}
	} 
	fdwrite(c->fd, resp, ret);
	shutdown(c->fd);
	close(c->fd);
}

int
build_error(char *msg, char **resp)
{
	*resp = malloc(strlen(msg));
	strcpy(*resp, msg);
	return strlen(msg);
}

int
put(DB *db, char *body, char **resp)
{
	int ret;
	DBT k, v;

	memset(&k, 0, sizeof(k));
	memset(&v, 0, sizeof(v));

	k.size = 36;
	k.data = body; 
	v.size = strlen(body) - 37;
	v.data = body + 37;

	if ((ret = db->put(db, NULL, &k, &v, 0)) != 0) {
		return ret;
	}

	*resp = k.data;
	return k.size;
}

int
get(DB *db, char *body, char **resp)
{
	int ret;
	DBT k, v;
	memset(&k, 0, sizeof(k));
	memset(&v, 0, sizeof(v));

	k.size = 36;
	k.data = body;
	
	if ((ret = db->get(db, NULL, &k, &v, 0)) != 0) {
		return ret;
	}

	*resp = v.data;
	return v.size;
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
		"/tmp/sfdb-env", 
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

	u_int32_t db_flags = DB_AUTO_COMMIT | DB_READ_UNCOMMITTED;
	ret = db->open(db, NULL, "sf.db", NULL, DB_UNKNOWN, db_flags, 0);
	if (ret != 0) {
		//If the error is ENOENT we should create the database.
		if (ret == 2) {
			printf("at=create-database\n");
			ret = db->open(db, 
					NULL, 
					"sf.db", 
					NULL, 
					DB_BTREE, 
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
