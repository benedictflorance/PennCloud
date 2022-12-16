all: smtp

smtp: smtp.cpp ../kvstore/libkvstore.a
	clang++ -g -std=c++17 -Wall -lpthread -lprotobuf -o smtp $^
