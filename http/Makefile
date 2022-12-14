all: server

server: http_server.cpp index.cpp
	clang++ -g -std=c++17 -Wall -lpthread -lprotobuf -o web_server $^