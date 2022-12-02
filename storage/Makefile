TARGETS = tablet_server tablet_client master client_wrapper

all: $(TARGETS)

storage: index.cpp 
	g++ $< -I../protobuf-21.9/src -lpthread -lprotobuf -std=c++17 -g -o $@
