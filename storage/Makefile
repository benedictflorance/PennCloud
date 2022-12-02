TARGETS = storage
all: $(TARGETS)

storage: index.cpp ../kvstore/client_wrapper.o ../kvstore/request.pb.cc ../kvstore/response.pb.cc
	g++ $< -std=c++17 -Wall -lpthread -lprotobuf -g -o $@
