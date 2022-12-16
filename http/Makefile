all: server load_balancer smtp

server: http_server.cpp index.cpp ../kvstore/libkvstore.a
	clang++ -g -std=c++17 -Wall -w -lpthread -lprotobuf -lresolv -o web_server $^

load_balancer: balancer/load_balance.cpp ../kvstore/libkvstore.a
	clang++ -O3 -std=c++17 -Wall -lpthread -o load_balancer $^

smtp: smtp.cpp ../kvstore/libkvstore.a
<<<<<<< HEAD
	clang++ -O3 -std=c++17 -Wall -lpthread -lprotobuf -o smtp $^
=======
	clang++ -O3 -std=c++17 -Wall -lprotobuf -lpthread -o smtp $^
>>>>>>> 86f3d4e78f77e3e3dd3ef0d240f646c4f2b46f60
