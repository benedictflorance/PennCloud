#include "client_wrapper.h"

KVstore kvstore;

KVstore::KVstore()
{
    for(int i = 0; i < NUM_SERVERS; i++)
    {
        is_server_alive[i] = true;
    }
}

sockaddr_in KVstore::get_address(std::string socket_address)
{
    int colon_index = socket_address.find(":");
    std::string ip_address = socket_address.substr(0, colon_index);
    std::string port_str = socket_address.substr(colon_index + 1, socket_address.length() - colon_index - 1);
    if(ip_address.empty())
    {
        exit(-1);
    }
    int port;
    try
    {
        port = stoi(port_str);
    }

    catch(const std::invalid_argument&)
    {
        exit(-1);
    }
    struct sockaddr_in servaddr;
    bzero(&servaddr, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(port);
    inet_pton(AF_INET, ip_address.c_str(), &(servaddr.sin_addr));
    return servaddr;
}
// function for reading
bool do_read(int fd, char *buf, int len){
  int rcvd = 0;
  while (rcvd < len)
  {
    int n = read(fd, &buf[rcvd], len - rcvd);
    if (n < 0)
      return false;
    rcvd += n;
  }
  return true;
}
bool do_write(int fd, char *buf, int len){
  int sent = 0;
  while (sent < len)
  {
    int n = write(fd, &buf[sent], len - sent);
    if (n < 0)
      return false;
    sent += n;
  }
  return true;
}
std::pair<std::string, std::string> KVstore::send_request(int sockfd, std::string type, std::string rkey, std::string ckey, std::string value1, std::string value2)
{
    std::string request_str;
    char *response_buffer = new char[BUFFER_SIZE];
    char length_buffer[LENGTH_BUFFER_SIZE];
	memset(length_buffer, 0, sizeof(length_buffer));
    memset(response_buffer, 0, sizeof(response_buffer));
    PennCloud::Request request;
    PennCloud::Response response;
    // Test invalid rowkey
    request.set_type(type);
    request.set_rowkey(rkey);
    request.set_columnkey(ckey);
    if(type == "PUT" || type == "CPUT")
    {
        request.set_value1(value1);
    }
    if(type == "CPUT")
    {
        request.set_value2(value2);;
    }
    request.SerializeToString(&request_str);

    char req_length[11];
    memset(req_length, 0, sizeof(req_length));
    snprintf (req_length, 11, "%10d", request_str.length()); 
    std::string message = std::string(req_length) + request_str;
    do_write(sockfd, message.data(), message.length());
    std::cout<<"Sending a tablet server request type of "<<request.type()<<" a rowkey of "<<request.rowkey()<<" a columnkey of "<<request.columnkey()<<" a value1 of "<<request.value1().length()
						<<" a value2 of "<<request.value2().length()<<std::endl; 
    do_read(sockfd, length_buffer, 10);
	int request_length = std::stoi(std::string(length_buffer, 10));
    do_read(sockfd, response_buffer, request_length);
    std::string response_buffer_str = std::string(response_buffer, request_length);
    response.ParseFromString(response_buffer_str);
    std::cout<<"Received a status of "<<response.status()<<" description of "<<response.description()<<" value of size "<<response.value().length()<<std::endl;
    std::pair<std::string, std::string> response_str = std::make_pair(response.value(), response.status());
    delete response_buffer;
    return response_str;
}
std::pair<std::string, std::string> KVstore::contact_tablet_server(std::string type, std::string rkey, std::string ckey, std::string value1, std::string value2)
{   
    int sockfd = socket(PF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) 
    {
        std::cerr<<"Unable to create socket"<<std::endl;
        return std::pair<std::string, std::string>("", "-CRASH");

    }
    if(connect(sockfd, (struct sockaddr*)& rkey_to_storage_cache[rkey], sizeof(rkey_to_storage_cache[rkey])) < 0)
    {
        // Tablet server crash
        rkey_to_storage_cache.erase(rkey);
        // Contact master again!
        close(sockfd);
        return std::pair<std::string, std::string>("", "-CRASH");
    }
    // Send request here
    std::pair<std::string, std::string> response_str = send_request(sockfd, type, rkey, ckey, value1, value2);
    close(sockfd);
    return response_str;
}
// clear cache every 2 minutes
std::pair<std::string, std::string> KVstore::process_kvstore_request(std::string type, std::string rkey, std::string ckey, std::string value1, std::string value2)
{
    bool is_crash = false;
    std::pair<std::string, std::string> response_str;
    do
    {
        // if rkey in cache, directly send it to storage server (if storage server cannot be connected, recontact master)
        if(rkey_to_storage_cache.find(rkey) != rkey_to_storage_cache.end())
        {
            response_str = contact_tablet_server(type, rkey, ckey, value1, value2);
        }
        // else send it to master and then send it to storage server 
        else
        {
            sockaddr_in master_sock_addr = get_address(master_ip_str);
            int sockfd = socket(PF_INET, SOCK_STREAM, 0);
            if (sockfd < 0) 
            {
                std::cerr<<"Unable to create socket"<<std::endl;
                return std::pair<std::string, std::string>("", "-MASTERCRASH");

            }
            connect(sockfd, (struct sockaddr*)& master_sock_addr, sizeof(master_sock_addr));
            // Send request here
            std::string rkey_request_msg = "REQ(" + rkey + ")\r\n";
            char* response_buffer = new char[BUFFER_SIZE];
            write(sockfd, rkey_request_msg.c_str(), strlen(rkey_request_msg.c_str()));
            while(read(sockfd, response_buffer, BUFFER_SIZE) == 0);
            std::string tablet_ip_str = std::string(response_buffer);
            // Cache it 
            std::cout<<tablet_ip_str<<std::endl;
            rkey_to_storage_cache[rkey] = get_address(tablet_ip_str);
            response_str = contact_tablet_server(type, rkey, ckey, value1, value2);
            delete response_buffer;
            close(sockfd);
        }
        // Contact master again if 
        if(response_str.second == "-CRASH")
        {
            is_crash = true;
        }
        else
        {
            is_crash = false;
        }
    } while (is_crash == true);  
    return response_str;  
}
std::string KVstore::get(std::string rkey, std::string ckey)
{
    std::pair<std::string, std::string> result = process_kvstore_request("GET", rkey, ckey);
    return result.first;

}
void KVstore::put(std::string rkey, std::string ckey, std::string value)
{
    if(value==""){
        dele(rkey, ckey);
        return;
    }
    process_kvstore_request("PUT", rkey, ckey, value);
}
bool KVstore::cput(std::string rkey, std::string ckey, std::string value1, std::string value2)
{
    if(value1 == ""){
        if(get(rkey, ckey) == "")
        {
            put(rkey, ckey, value2);
            return true;
        }
        else
            return false;
    }
    std::pair<std::string, std::string> result = process_kvstore_request("CPUT", rkey, ckey, value1, value2);
    if(result.second == "+OK")
        return true;
    else
        return false;
}
bool KVstore::dele(std::string rkey, std::string ckey)
{
    std::pair<std::string, std::string> result = process_kvstore_request("DELETE", rkey, ckey);
    if(result.second == "+OK")
        return true;
    else
        return false;
}

void KVstore::kill(int server_index){
    sockaddr_in master_sock_addr = get_address(master_ip_str);
    int sockfd = socket(PF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) 
    {
        std::cerr<<"Unable to create socket"<<std::endl;
        return;
    }
    connect(sockfd, (struct sockaddr*)& master_sock_addr, sizeof(master_sock_addr));
    // Send request here
    std::string rkey_request_msg = "KILL(" + std::to_string(server_index) + ")\r\n";
    write(sockfd, rkey_request_msg.c_str(), strlen(rkey_request_msg.c_str()));
    is_server_alive[server_index] = false;
    rkey_to_storage_cache.clear();
}

void KVstore::resurrect(int server_index){
    sockaddr_in master_sock_addr = get_address(master_ip_str);
    int sockfd = socket(PF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) 
    {
        std::cerr<<"Unable to create socket"<<std::endl;
        return;
    }
    connect(sockfd, (struct sockaddr*)& master_sock_addr, sizeof(master_sock_addr));
    // Send request here
    std::string rkey_request_msg = "RESURRECT(" + std::to_string(server_index) + ")\r\n";
    write(sockfd, rkey_request_msg.c_str(), strlen(rkey_request_msg.c_str()));
    is_server_alive[server_index] = true;
}

// Sample Test
void test()
{
    KVstore kv_test;

    // kv_test.kill(0);
    // kv_test.kill(3);
    // kv_test.resurrect(6);
    // kv_test.kill(7);
    // kv_test.kill(8);
    // kv_test.resurrect(0);

    // std::cout<<"Starting test"<<std::endl;
    // kv_test.put("0hanbang", "password", "frontend");
    // std::string response_str = kv_test.get("0hanbang", "password");
    // std::cout<<response_str<<std::endl;
    // bool is_success = kv_test.cput("0hanbang", "password", "frontend", "backend");
    // std::cout<<is_success<<std::endl;

    // response_str = kv_test.get("0hanbang", "password");
    // std::cout<<response_str<<std::endl;
    // is_success = kv_test.dele("0hanbang", "password");
    // std::cout<<is_success<<std::endl;
    // response_str = kv_test.get("0hanbang", "password");
    // std::cout<<response_str<<std::endl;
    // kv_test.put("0hanbang", "password", "frontend"); // Expected nothing 
    // response_str = kv_test.get("0hanbang", "password");
    // std::cout<<response_str<<std::endl; // Expected frontend




    // is_success = kv_test.cput("Shanbang", "password", "", "backend");
    // std::cout<<is_success<<std::endl; // Expected 0
    // is_success = kv_test.cput("15hanbang", "password", "", "backend");
    // std::cout<<is_success<<std::endl; // Expected 1
    // response_str = kv_test.get("Shanbang", "password");
    // std::cout<<response_str<<std::endl; // Expected frontend
    // kv_test.put("Shanbang", "password","");
    // response_str = kv_test.get("Shanbang", "password"); // Expected ""
    // std::cout<<response_str<<std::endl;
}