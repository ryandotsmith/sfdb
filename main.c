#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <task.h>
#include <db.h>

int default_srv_port = 8000;
int default_group_creator = 1;
char *default_data_dir = "/tmp/sfdb-env";
char default_local_addr[] = "localhost:5000";
char default_remote_addr[] = "localhost:5001";

enum
{
        STACK = 32768
};

typedef struct {
	char *local;
	char *remote;
	char *data_dir;
	int group_creator;
	int srv_port;
} options;

//This struct makes it handy to pass state to
//an async task. See taskmain and handle_req for usage.
typedef struct {
	DB_ENV *env;
	DB *db;
	int fd;
} conn;

//Returns the port portion of an address:port string.
int
stoport(char *s)
{
	char *loc;
	loc = strchr(s, ':');
	if (loc != NULL) {
		return atoi(loc + 1);
	}
	return 5555;
}

//Returns the host portion of an address:port string.
//This function mutates str truncating the string after ':' including the ':'.
char *
stohost(char *str)
{
	char *loc;
	loc = strchr(str, ':');
	if (loc != NULL) {
		*loc = 0;
		return str;
	}
	return "localhost";
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

int
parselpbytes(char *dest, char *src)
{
	const char max_len = 1;
	char lens[max_len];
	int len;

	memmove(lens, src, max_len);
	len = atoi(lens);

	dest = malloc(len);
	memmove(dest, src + max_len, len);
	return len;
}

int
cas(DB_ENV *env, DB *db, char *id, char *body, char **resp)
{
	char expected[1], new[1];
	DBT k, v, actual;
	DB_TXN *txn;
	int new_sz, exists, expected_sz, ret;

	memset(&k, 0, sizeof(k));
	memset(&v, 0, sizeof(v));
	memset(&actual, 0, sizeof(actual));

	if ((ret = env->txn_begin(env, NULL, &txn, 0)) != 0) {
		return ret;
	}

	k.flags = DB_DBT_MALLOC;
	v.flags = DB_DBT_MALLOC;
	actual.flags = DB_DBT_MALLOC;

	k.size = 36;
	k.data = id;

	//Hang on to the length of the expected
	//value so we can use it as an offset to retreive the new value.
	expected_sz = parselpbytes(expected, body);
	new_sz = parselpbytes(new, body + (expected_sz + 1));

	//If the key doesn't exist, then we can safely put.
	if ((db->exists(db, txn, &k, 0)) != 0) {
		v.size = new_sz;
		v.data = new;
		if ((ret = db->put(db, txn, &k, &v, 0)) != 0) {
			return ret;
		}
		if (ret = (txn->commit(txn, 0)) != 0) {
			return ret;
		}
		*resp = v.data;
		return v.size;
	}

	//Fetch the current value of the key.
	//We will use the result to compare with the expected
	//value parsed from the body of the request.
	if ((ret = db->get(db, txn, &k, &actual, 0)) != 0) {
		return ret;
	}
	if (strncmp(expected, (char *)actual.data, expected_sz) == 0) {
		v.size = new_sz;
		v.data = new;
		if ((ret = db->put(db, txn, &k, &v, 0)) != 0) {
			return ret;
		}
		if (ret = (txn->commit(txn, 0)) != 0) {
			return ret;
		}
		*resp = v.data;
		return v.size;
	}

	if (ret = (txn->abort(txn)) != 0) {
		return ret;
	}

	//The compare step failed.
	return -1;
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

int
process_cmd(conn *c, char *cmd, char *id, char *body, char **resp)
{
	int ret;
	switch(*cmd) {
	case 'p':
		if ((ret = put(c->db, id, body, resp)) <= 0) {
			fprintf(stderr, "error=%s\n", db_strerror(ret));
		}
		break;
	case 'g':
		if ((ret = get(c->db, id, resp)) <= 0) {
			fprintf(stderr, "error=%s\n", db_strerror(ret));
		}
		break;
	case 'c':
		if ((ret = cas(c->env, c->db, id, body, resp)) <= 0) {
			fprintf(stderr, "cas-error=%s\n", db_strerror(ret));
		}
		break;
	}
	return ret;
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
		ret = process_cmd(c, cmd, id, body, &resp);
		int sz = sizeof ver + sizeof cmd + sizeof id + ret;
		char final[sizeof lens + sz];
		sprintf(final, "%06d%*c%*c%*s%*s",
			sz, 1, '1', 1, 's', 36, id, ret, resp);
		fdwrite(c->fd, final, strlen(final));
		free(buff);
	}
	shutdown(c->fd);
	close(c->fd);
}

int
init_env(DB_ENV **dbenvp, options *opts)
{
	DB_ENV *dbenv;
	int ret;
	if (ret = db_env_create(&dbenv, 0) != 0) {
		return ret;
	}
	ret = dbenv->open(dbenv,
		opts->data_dir,
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

	u_int32_t db_flags = DB_AUTO_COMMIT ;
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
init_site(DB_ENV *env, options *opts)
{
	int ret;
	char *lhost, *rhost;
	int lport, rport;

	lhost = stohost(opts->local);
	lport = stoport(opts->local);
	rhost = stohost(opts->remote);
	rport = stoport(opts->remote);

	DB_SITE *site;
	if ((ret = env->repmgr_site(env, lhost, lport, &site, 0)) != 0) {
		return ret;
	}

	site->set_config(site, DB_LOCAL_SITE, 1);

	if (opts->group_creator) {
		site->set_config(site, DB_GROUP_CREATOR, 1);
	}
	if ((ret = site->close(site)) != 0) {
		return ret;
	}

	if (!opts->group_creator) {
		ret = env->repmgr_site(env, rhost, rport, &site, 0);
		if (ret != 0) {
			return ret;
		}
		site->set_config(site, DB_BOOTSTRAP_HELPER, 1);
		if ((ret = site->close(site)) != 0) {
			return ret;
		}
	}
	return 0;
}

void
parse_opts(int argc, char **argv, options *opts) {
	int c;
	while ((c = getopt(argc, argv, "l:r:d:p:")) != -1) {
		switch (c) {
			case 'd':
				opts->data_dir = optarg;
			case 'p':
				opts->srv_port = atoi(optarg);
				break;
			case 'l':
				opts->local = optarg;
				break;
			case 'r':
				opts->remote = optarg;
				break;
		}
	}
	return;
}

//libtask takes care of setting up the *main* function. The library
//requires that a function named taskmain be defined instead of main.
void
taskmain(int argc , char **argv)
{
	options opts;
	opts.srv_port = default_srv_port;
	opts.data_dir = default_data_dir;
	opts.local = default_local_addr;
	opts.remote = default_remote_addr;
	opts.group_creator = default_group_creator;
	parse_opts(argc, argv, &opts);

	DB_SITE *site;
	DB_ENV *env;
	DB *db;

	int ret;
	if ((ret = init_env(&env, &opts)) != 0) {
		fprintf(stderr, "init_env error: %s\n", db_strerror(ret));
		exit(1);
	}
	printf("at=env-initialized\n");

	if ((ret = init_site(env, &opts)) != 0) {
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

	fd = netannounce(TCP, 0, opts.srv_port);
	if (fd < 0) {
		fprintf(stderr, "Unable to announce on port 8000.\n");
		exit(1);
	}
	fdnoblock(fd);
	while((cfd = netaccept(fd, remote, &rport)) >= 0) {
		printf("at=client-connect remote=%s\n", remote);
		conn *c = malloc(sizeof(conn));
		c->db = db;
		c->env = env;
		c->fd = cfd;
		taskcreate(handle_req, (void *)c, STACK);
	}
	return;
}
