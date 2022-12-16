TARGETS = webmail

all: $(TARGETS)


webmail: index.cpp 
	g++ $< mail.pb.cc -O3 ../kvstore/client_wrapper.o  ../http/http_server.cpp ../kvstore/request.pb.cc ../kvstore/response.pb.cc -std=c++17 -Wall -lpthread -lcrypto -lprotobuf  -o $@ 