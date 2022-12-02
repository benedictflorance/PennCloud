all: server

storage: index.cpp ../kvstore/client_wrapper.o ../kvstore/request.pb.cc ../kvstore/response.pb.cc ../http_server/http_server.cpp
	clang++ -std=c++17 -Wall -lpthread -lprotobuf -o web_server $^