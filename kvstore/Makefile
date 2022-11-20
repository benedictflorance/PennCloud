TARGETS = tablet_server tablet_client master

all: $(TARGETS)

tablet_server: tablet_server.cpp utils.h globalvars.h
	g++ $< -std=c++14 request.pb.cc -lpthread -I/usr/local/lib -lprotoc -lprotobuf -std=c++17 -g -o $@

tablet_client: tablet_client.cpp utils.h globalvars.h
	g++ $< request.pb.cc -lpthread -I/usr/local/lib -lprotoc -lprotobuf -std=c++17 -g -o $@

master: master.cpp
	g++ $^ -std=c++14 -lpthread -g -o $@
