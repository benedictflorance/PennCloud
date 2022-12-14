#pragma once

#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <string>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include "request.pb.h"
#include "response.pb.h"

class KVstore
{
    std::string master_ip_str = "127.0.0.1:8000"; 
    const int STATUS_BUFFER_SIZE = 1000;
    const int LENGTH_BUFFER_SIZE = 20; 
    const int NUM_SERVERS = 9;
    std::map<int, bool> is_server_alive;
    std::unordered_map<std::string, sockaddr_in> rkey_to_storage_cache;
    std::pair<std::string, std::string> send_request(int sockfd, const std::string &type, const std::string &rkey, const std::string &ckey, const std::string &value1, const std::string &value2);
    std::pair<std::string, std::string> contact_tablet_server(const std::string &type, const std::string &rkey, const std::string &ckey, const std::string &value1, const std::string &value2);
    std::pair<std::string, std::string> process_kvstore_request(const std::string &type, const std::string &rkey, const std::string &ckey, const std::string &value1, const std::string &value2);
    sockaddr_in get_address(std::string socket_address);

    public:
        KVstore();
        std::string get(const std::string &rkey, const std::string &ckey);
        void put(const std::string &rkey, const std::string &ckey, const std::string &value);
        bool cput(const std::string &rkey, const std::string &ckey, const std::string &value1, const std::string &value2);
        bool dele(const std::string &rkey, const std::string &ckey);
        void kill(int server_index);
        void resurrect(int server_index);
        std::vector<bool> list_server_status();
        std::vector<std::string> list_rowkeys();
        std::vector<std::string> list_colkeys(const std::string &rkey);
};

extern KVstore kvstore;
