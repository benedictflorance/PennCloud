TARGETS = tablet_server tablet_client master client_wrapper

all: $(TARGETS)

tablet_server: tablet_server.cpp request.pb.cc response.pb.cc utils/*
	g++ $< request.pb.cc response.pb.cc -lpthread -lprotobuf -std=c++17 -g -o $@

tablet_client: tablet_client.cpp request.pb.cc response.pb.cc utils/*
	g++ $< request.pb.cc response.pb.cc -lpthread -lprotobuf -std=c++17 -g -o $@

client_wrapper: client_wrapper.cpp request.pb.cc response.pb.cc 
	g++ $^ -lpthread -lprotobuf -std=c++17 -g -c

master: master.cpp
	g++ $^ -std=c++17 -lpthread -w -g -o $@
