CC=gcc
FLAGS=-I/usr/local/BerkeleyDB.6.0/include -Wl,-rpath /usr/local/BerkeleyDB.6.0/lib -L/usr/local/BerkeleyDB.6.0/lib

default: clean
	$(CC) $(FLAGS) main.c -ldb -ltask

clean:
	rm -f a.out
	rm -rf /tmp/sfdb-env
	mkdir -p /tmp/sfdb-env
