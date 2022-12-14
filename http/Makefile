all: server

server: http_server.cpp index.cpp ../kvstore/libkvstore.a
	clang++ -g -std=c++17 -Wall -lpthread -lprotobuf -o web_server $^
