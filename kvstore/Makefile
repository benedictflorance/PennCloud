TARGETS = tablet_server tablet_client master

all: $(TARGETS)

tablet_server: tablet_server.cpp utils/config_processor.h utils/*
	g++ $< request.pb.cc response.pb.cc -lpthread -I/usr/local/lib -lprotoc -lprotobuf -std=c++17 -g -o $@

tablet_client: tablet_client.cpp utils/config_processor.h utils/globalvars.h
	g++ $< request.pb.cc response.pb.cc -lpthread -I/usr/local/lib -lprotoc -lprotobuf -std=c++17 -g -o $@

master: master.cpp
	g++ $^ -std=c++14 -lpthread -g -o $@
