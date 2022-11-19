TARGETS = tablet_server tablet_client

all: $(TARGETS)

tablet_server: tablet_server.cpp utils.h globalvars.h
	g++ $< -lpthread -std=c++17 -g -o $@

tablet_client: tablet_client.cpp utils.h globalvars.h
	g++ $< -lpthread -I/usr/local/include -lprotoc -lprotobuf -std=c++17 -g -o $@
