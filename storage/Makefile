all: server

storage: index.cpp 
	g++ $< -g ../kvstore/client_wrapper.cpp  ../http/http_server.cpp  ../kvstore/request.pb.cc ../kvstore/response.pb.cc  -std=c++17 -Wall -lpthread -lprotobuf  -o $@ 
