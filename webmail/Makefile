TARGETS = smtp pop3 echoserver smtp-ec deliver-mails

all: $(TARGETS)


webmail: index.cpp 
	g++ $< mail.pb.cc -g ../kvstore/client_wrapper.o  ../http/http_server.cpp ../kvstore/request.pb.cc ../kvstore/response.pb.cc -std=c++17 -Wall -lpthread -lcrypto -lprotobuf  -o $@ 

echoserver: echoserver.cc
	g++ $^ -lpthread -g -o $@

smtp: smtp.cc
	g++ $< -lpthread -g -o $@

smtp-ec: smtp-ec.cc
	g++ $< -lpthread -g -o $@

deliver-mails: deliver-mails.cc
	g++ $< -lpthread -lresolv -g -o $@

pop3: pop3.cc
	g++ $^ -I/usr/local/opt/openssl/include -L/usr/local/opt/openssl/lib -lcrypto -lpthread -g -o $@

pack:
	rm -f submit-hw2.zip
	zip -r submit-hw2.zip *.cc README Makefile

clean::
	rm -fv $(TARGETS) *~

realclean:: clean
	rm -fv submit-hw2.zip
