all: storage

storage: index.cpp ../kvstore/libkvstore.a
	g++ $< -g -w ../kvstore/client_wrapper.cpp  ../http/http_server.cpp  ../kvstore/request.pb.cc ../kvstore/response.pb.cc  -std=c++17 -Wall -lpthread -lprotobuf  -o $@ 
