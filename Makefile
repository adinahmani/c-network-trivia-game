# Makefile for building the trivia server and client

CC       := gcc
CFLAGS   := -Wall -Wextra -g
LDFLAGS  := -pthread

# Binary names
SERVER   := Server
CLIENT   := Client

# Source files
SRCS_S   := Server.c
SRCS_C   := Client.c

.PHONY: all clean

all: $(SERVER) $(CLIENT)

$(SERVER): $(SRCS_S)
	$(CC) $(CFLAGS) $< -o $@ $(LDFLAGS)

$(CLIENT): $(SRCS_C)
	$(CC) $(CFLAGS) $< -o $@ $(LDFLAGS)

clean:
	-rm -f $(SERVER) $(CLIENT)

