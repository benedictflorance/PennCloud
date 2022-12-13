TARGETS = tablet_server tablet_client master client_wrapper #libkvstore.a #

all: $(TARGETS)

tablet_server: tablet_server.cpp request.pb.cc response.pb.cc utils/*
	g++ $< request.pb.cc response.pb.cc -lpthread -w -lprotobuf -std=c++17 -g -o $@

tablet_client: tablet_client.cpp request.pb.cc response.pb.cc utils/*
	g++ $< request.pb.cc response.pb.cc -lpthread -w -lprotobuf -std=c++17 -g -o $@

client_wrapper: client_wrapper.cpp request.pb.cc response.pb.cc utils/*
	g++ $< request.pb.cc response.pb.cc -lpthread -w -lprotobuf -std=c++17 -g -o $@

# client_wrapper.o: client_wrapper.cpp request.pb.cc response.pb.cc 
# 	g++ $^ -lpthread -lprotobuf -std=c++17 -w -g -c

# libkvstore.a: client_wrapper.o
# 	ar rcs libkvstore.a client_wrapper.o request.pb.o response.pb.o

master: master.cpp
	g++ $^ request.pb.cc response.pb.cc -std=c++17 -lprotobuf -lpthread -w -g -o $@
