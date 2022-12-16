TARGETS = tablet_server tablet_client master libkvstore.a #client_wrapper

all: $(TARGETS)

tablet_server: tablet_server.cpp request.pb.cc response.pb.cc utils/*
	g++ $< request.pb.cc response.pb.cc -lpthread -w -lprotobuf -I/usr/local/opt/openssl/include -L/usr/local/opt/openssl/lib -lcrypto -std=c++17 -O3 -o $@

tablet_client: tablet_client.cpp request.pb.cc response.pb.cc utils/*
	g++ $< request.pb.cc response.pb.cc -lpthread -w -lprotobuf -I/usr/local/opt/openssl/include -L/usr/local/opt/openssl/lib -lcrypto -std=c++17 -O3 -o $@

# client_wrapper: client_wrapper.cpp request.pb.cc response.pb.cc utils/*
# 	g++ $< request.pb.cc response.pb.cc -lpthread -w -lprotobuf -std=c++17 -O3 -o $@

client_wrapper.o: client_wrapper.cpp request.pb.cc response.pb.cc 
	g++ $^ -lpthread -lprotobuf -std=c++17 -w -O3 -c

libkvstore.a: client_wrapper.o
	ar rcs libkvstore.a client_wrapper.o request.pb.o response.pb.o

master: master.cpp
	g++ $^ request.pb.cc response.pb.cc -std=c++17 -lprotobuf -lpthread -w -I/usr/local/opt/openssl/include -L/usr/local/opt/openssl/lib -lcrypto -O3 -o $@
