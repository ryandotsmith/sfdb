CC=gcc
FLAGS=-I/home/action/usr/include -Wl,-rpath /home/action/usr/lib -L/home/action/usr/lib  

default: clean
	$(CC) $(FLAGS) kq.c -ldb

clean:
	rm -f a.out
	rm -f /tmp/kq-env/*
