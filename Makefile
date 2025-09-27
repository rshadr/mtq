all: mtq

mtq: mtq.c
	gcc -o mtq -O2 -ggdb3 -Wall -Wextra -pedantic mtq.c -lpthread

clean:
	rm mtq

.PHONY: all clean

