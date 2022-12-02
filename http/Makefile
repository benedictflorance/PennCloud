all: server

server: http_server.cpp index.cpp ../kvstore/client_wrapper.o ../kvstore/request.pb.cc ../kvstore/response.pb.cc
	clang++ -std=c++17 -Wall -lpthread -lprotobuf -o web_server $^