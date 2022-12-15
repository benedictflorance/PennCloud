all: server load_balancer

server: http_server.cpp index.cpp ../kvstore/libkvstore.a
	clang++ -g -std=c++17 -Wall -w -lpthread -lprotobuf -lresolv -o web_server $^

load_balancer: balancer/load_balance.cpp ../kvstore/libkvstore.a
	clang++ -O3 -std=c++17 -Wall -lpthread -o load_balancer $^
