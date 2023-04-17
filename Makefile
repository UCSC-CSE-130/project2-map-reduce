CC=clang
CFLAGS=-Wall -Wextra -std=c11 -pedantic

all: word-count

word-count: word-count.c kvlist.o mr.o hash.o

%.o : %.c %.h
	$(CC) $(CFLAGS) $< -c

clean:
	-rm word-count *.o
