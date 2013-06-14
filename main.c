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

//This struct makes it handy to pass state to
//an async task. See taskmain and handle_req for usage.
typedef struct {
	DB_ENV *env;
	DB *db;
	int fd;
} conn;

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

int
put(DB *db, char *id, char *body, char **resp)
{
	int ret;
	DBT k, v;

	memset(&k, 0, sizeof(k));
	memset(&v, 0, sizeof(v));

	k.size = 36;
	k.data = id; 
	k.flags = DB_DBT_MALLOC;
	v.size = strlen(body) + 1;
	v.data = body;
	v.flags = DB_DBT_MALLOC;

	if ((ret = db->put(db, NULL, &k, &v, 0)) != 0) {
		return ret;
	}
	*resp = v.data;
	return v.size;
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
	k.flags = DB_DBT_MALLOC;

	v.flags = DB_DBT_MALLOC;
	
	if ((ret = db->get(db, NULL, &k, &v, 0)) != 0) {
		return ret;
	}

	*resp = v.data;
	return v.size;
}

void
handle_req(void *v)
{
	int len, ret, n, offset;
	char *buff, *resp; //buff, body, and resp sizes not known.
	char lens[6]; //each request/response prefixed with length max 9999.
        char cmd[1];  //command parsed from the request.
	char ver[1];  //version parsed from the request.
	char id[36];  //id is commonly in uuid format.
	conn *c;
	c = (conn *)v;

	while(1) {
		n = 0;
		if((n = readnbytes(c->fd, 6, lens)) == 0) {
			break;
		}
		len = atoi(lens);

		buff = malloc(len);
		if ((n = readnbytes(c->fd, len, buff)) == 0) {
			break;
		}
		strncpy(ver, buff, 1);
		strncpy(cmd, buff + 1, 1);
		strncpy(id, buff + 2, 36);
		char body[len - 38];
		strncpy(body, buff + 38, len - 38);

		if (*cmd == 'p') {
			if ((ret = put(c->db, id, body, &resp)) <= 0) {
				fprintf(stderr, "error=%s\n", db_strerror(ret));
			}
		} else if (*cmd == 'g') {
			if ((ret = get(c->db, id, &resp)) <= 0) {
				fprintf(stderr, "error=%s\n", db_strerror(ret));
			}
		} 

		int sz = sizeof ver + sizeof cmd + sizeof id + ret;
		char final[sizeof lens + sz];
		sprintf(final, "%06d%*c%*c%*s%*s", 
			sz, 1, '1', 1, 's', 36, id, ret, resp); 
		fdwrite(c->fd, final, strlen(final));
		free(buff);
	}
	printf("disconnected\n");
	shutdown(c->fd);
	close(c->fd);
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
		DB_INIT_REP |
		DB_THREAD |
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

int
init_site(DB_ENV *env, int creator)
{
	int ret;

	char *lhost = "localhost";
	int lport = 5000;
	char *ohost = "localhost";
	int oport = 5001;

	DB_SITE *site;
	if ((ret = env->repmgr_site(env, lhost, lport, &site, 0)) != 0) {
		return ret;
	}

	site->set_config(site, DB_LOCAL_SITE, 1);

	if (creator) {
		site->set_config(site, DB_GROUP_CREATOR, 1);
	}
	if ((ret = site->close(site)) != 0) {
		return ret;
	}

	if (!creator) {
		ret = env->repmgr_site(env, ohost, oport, &site, 0);
		if (ret != 0) {
			return ret;
		}
		site->set_config(site, DB_BOOTSTRAP_HELPER, 1);
		if ((ret = site->close(site)) != 0) {
			return ret;
		}
	}
}

void
taskmain(int argc , char **argv)
{
	DB_SITE *site;
	DB_ENV *env;
	DB *db;

	int ret;
	if ((ret = init_env(&env)) != 0) {
		fprintf(stderr, "init_env error: %s\n", db_strerror(ret));
		exit(1);
	}
	printf("at=env-initialized\n");

	if ((ret = init_site(env, 1)) != 0) {
		fprintf(stderr, "init_db_site error: %s\n", db_strerror(ret));
		exit(1);
	}
	printf("at=rep-initialized\n");

	if ((ret = init_db(&db, env)) != 0) {
		fprintf(stderr, "init_db error: %s\n", db_strerror(ret));
		exit(1);
	}
	printf("at=db-initialized\n");

	//Use 3 threads for processing replication events.
	if ((ret = env->repmgr_start(env, 3, DB_REP_ELECTION)) != 0) {
		fprintf(stderr, "repmgr error: %s\n", db_strerror(ret));
		exit(1);
	}

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
