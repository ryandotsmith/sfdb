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

typedef struct {
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
	int len, ret, n;
	char *buff, *body, *resp; //buff, body, and resp sizes not known.
	char lens[4]; //each request/response prefixed with length max 9999.
        char cmd[1];  //command parsed from the request.
	char ver[1];  //version parsed from the request.
	char id[36];  //id is commonly in uuid format.
	conn *c;
	c = (conn *)v;

	while(1) {
		printf("at=new-command\n");
		memset(&lens, 0, sizeof lens);
		memset(&cmd, 0, sizeof cmd);
		memset(&ver, 0, sizeof ver);
		n = 0;

		if((n = readnbytes(c->fd, 4, lens)) == 0) {
			break;
		}
		len = atoi(lens);
		printf("length=%d\n", len);

		buff = malloc(len);
		if ((n = readnbytes(c->fd, len, buff)) == 0) {
			break;
		}

		strncpy(ver, buff, 1);
		if (*ver != '1') {
			char *msg = "1 e unsupported version.";
			fdwrite(c->fd, msg, strlen(msg) + 1);
			break;
		}
		printf("version=%s\n", ver);

		strncpy(cmd, buff + 1, 1);
		printf("cmd=%s\n", cmd);

		strncpy(id, buff + 2, 36);
		printf("id=%s\n", id);

		body = malloc(len - sizeof ver - sizeof cmd - sizeof id);
		memcpy(body, buff + 38, len - 38);
		printf("body=%s\n", body);

		if (*cmd == 'p') {
			if ((ret = put(c->db, id, body, &resp)) <= 0) {
				ret = build_error("e unable to put", &resp);
				fprintf(stderr, "error=%s\n", db_strerror(ret));
			}
		} else if (*cmd == 'g') {
			if ((ret = get(c->db, id, &resp)) <= 0) {
				ret = build_error("e unable to put", &resp);
				fprintf(stderr, "error=%s\n", db_strerror(ret));
			}
		} 

		printf("ret=%d resp=%s\n", ret, resp);
		int finalsz = sizeof ver + sizeof cmd + sizeof id + ret;
		char *final = malloc(finalsz);
		//length of reply is finalsz - space allocated for length (4).
		snprintf(final, finalsz, "%04d%c%c%s%s", 
			finalsz - 4, '1', 's', id, resp);
		printf("final=%s\n", final);
		write(c->fd, final, finalsz);
		free(final);
	}
	printf("disconnected\n");
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
put(DB *db, char *id, char *body, char **resp)
{
	int ret;
	DBT k, v;

	memset(&k, 0, sizeof(k));
	memset(&v, 0, sizeof(v));

	k.size = 36;
	k.data = id; 
	v.size = strlen(body);
	v.data = body;

	if ((ret = db->put(db, NULL, &k, &v, 0)) != 0) {
		return ret;
	}
	printf("at=put id=%s\n", (char *)k.data);
	*resp = k.data;
	return k.size;
}

int
get(DB *db, char *id, char **resp)
{
	int ret;
	DBT k, v;
	memset(&k, 0, sizeof(k));
	memset(&v, 0, sizeof(v));

	k.size = 36;
	k.data = id;
	
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
