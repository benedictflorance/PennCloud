all: server

server: http_server.cpp index.cpp
	clang++ -std=c++17 -Wall -lpthread -o web_server $^