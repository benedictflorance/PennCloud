all: server
server: http_server.cpp index.cpp mail.pb.cc
	clang++ -g -std=c++17 -Wall -lpthread -lprotobuf -lcrypto -o web_server $^


#server: http_server.cpp index.cpp ../kvstore/libkvstore.a mail.pb.cc
#	clang++ -g -std=c++17 -Wall -lpthread -lprotobuf -lcrypto -o web_server $^
