all: server load_balancer

server: http_server.cpp index.cpp ../kvstore/libkvstore.a mail.pb.cc
	clang++ -g -std=c++17 -Wall -lpthread -lprotobuf -lcrypto -o web_server $^

load_balancer: balancer/load_balance.cpp
	clang++ -O3 -std=c++17 -Wall -lpthread -o load_balancer $^
