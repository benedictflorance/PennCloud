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
std::pair<std::string, std::string> KVstore::send_request(int sockfd, const std::string &type, const std::string &rkey, const std::string &ckey, const std::string &value1, const std::string &value2)
{
    std::string request_str;
    char length_buffer[LENGTH_BUFFER_SIZE];
	memset(length_buffer, 0, sizeof(length_buffer));
    PennCloud::Request request;
    PennCloud::Response response;
    // Test invalid rowkey
    request.set_type(type);
    request.set_rowkey(rkey);
    request.set_columnkey(ckey);
    if(type == "PUT" || type == "CPUT" || type == "CREATE" || type == "RENAME")
    {
        request.set_value1(value1);
    }
    if(type == "CPUT" || type == "CREATE"|| type == "RENAME")
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
    char *response_buffer = new char[request_length+10];
    memset(response_buffer, 0, sizeof(response_buffer));
    do_read(sockfd, response_buffer, request_length);
    std::string response_buffer_str = std::string(response_buffer, request_length);
    response.ParseFromString(response_buffer_str);
    std::cout<<"Received a status of "<<response.status()<<" description of "<<response.description()<<" value of size "<<response.value().length()<<std::endl;
    std::pair<std::string, std::string> response_str;
    //CHANGE:
    if(request.type() == "LIST_COLKEY")
        response_str  = std::make_pair(response_buffer_str, response.status());
    else if(request.type() == "LIST_ROWKEY")
        response_str  = std::make_pair(response_buffer_str, response.status());
    else
        response_str  = std::make_pair(response.value(), response.status()); 
    delete response_buffer; 
    return response_str;
}
std::pair<std::string, std::string> KVstore::contact_tablet_server(const std::string &type, const std::string &rkey, const std::string &ckey, const std::string &value1, const std::string &value2)
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
std::pair<std::string, std::string> KVstore::process_kvstore_request(const std::string &type, const std::string &rkey, const std::string &ckey, const std::string &value1, const std::string &value2)
{
    bool is_crash = false;
    std::pair<std::string, std::string> response_str;
    do
    {
        // if rkey in cache, directly send it to storage server (if storage server cannot be connected, recontact master)
        if(rkey_to_storage_cache.find(rkey) != rkey_to_storage_cache.end())
        {
            std::string curr_ip_addr = std::string(inet_ntoa(rkey_to_storage_cache[rkey].sin_addr)) + ":" + 
            std::to_string(ntohs(rkey_to_storage_cache[rkey].sin_port));
            std::cout<<"Client connected to: "<<curr_ip_addr<<std::endl;
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
            char response_buffer[100];
            memset(response_buffer, 0, sizeof(response_buffer));  
            write(sockfd, rkey_request_msg.c_str(), strlen(rkey_request_msg.c_str()));
            while(read(sockfd, response_buffer, 100) == 0);
            std::string tablet_ip_str = std::string(response_buffer);
            // Cache it 
            std::cout<<"Client connected to: "<<tablet_ip_str<<std::endl;
            rkey_to_storage_cache[rkey] = get_address(tablet_ip_str);
            response_str = contact_tablet_server(type, rkey, ckey, value1, value2);
            close(sockfd);
        }
        // Contact master again if 
        if(response_str.second == "-CRASH")
        {
            std::cout<<"This server crashed. Re-requesting server from master"<<std::endl;
            is_crash = true;
            rkey_to_storage_cache.clear();
        }
        else
        {
            is_crash = false;
        }
    } while (is_crash == true);  
    return response_str;  
}
std::string KVstore::get(const std::string &rkey, const std::string &ckey)
{
    std::string empty = "";
    std::pair<std::string, std::string> result = process_kvstore_request("GET", rkey, ckey, empty, empty);
    return result.first;

}
void KVstore::put(const std::string &rkey, const std::string &ckey, const std::string &value)
{
    std::string empty = "";
    if(value==""){
        dele(rkey, ckey);
        return;
    }
    process_kvstore_request("PUT", rkey, ckey, value, empty);
}
bool KVstore::cput(const std::string &rkey, const std::string &ckey, const std::string &value1, const std::string &value2)
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
bool KVstore::dele(const std::string &rkey, const std::string &ckey)
{
    std::string empty = "";
    std::pair<std::string, std::string> result = process_kvstore_request("DELETE", rkey, ckey, empty, empty);
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
    // is_server_alive[server_index] = false;
    // rkey_to_storage_cache.clear();
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
    // is_server_alive[server_index] = true;
}

std::vector<bool> KVstore::list_server_status(){
    // Contact master to find out
    char length_buffer[LENGTH_BUFFER_SIZE];
    std::vector<bool> list_server;
    char status_buffer[STATUS_BUFFER_SIZE];
	memset(length_buffer, 0, sizeof(length_buffer));
    memset(status_buffer, 0, sizeof(status_buffer));    
    sockaddr_in master_sock_addr = get_address(master_ip_str);
    int sockfd = socket(PF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) 
    {
        std::cerr<<"Unable to create socket, returning old values"<<std::endl;
    }
    else
    {
        connect(sockfd, (struct sockaddr*)& master_sock_addr, sizeof(master_sock_addr));
        // Send request here
        std::string serv_status_msg = "STATUS\r\n";
        write(sockfd, serv_status_msg.c_str(), strlen(serv_status_msg.c_str()));
        do_read(sockfd, length_buffer, 10);
        int request_length = std::stoi(std::string(length_buffer, 10));
        char *response_buffer = new char[request_length+10];
        memset(response_buffer, 0, sizeof(response_buffer));
        do_read(sockfd, status_buffer, request_length);
        std::string status_buffer_str = std::string(status_buffer, request_length);
        PennCloud::Response response;
        response.ParseFromString(status_buffer_str);
        for(auto item : (*response.mutable_server_status()))
        {
            is_server_alive[item.first] = item.second;
        }
        for (auto& t : is_server_alive){
            std::cout << t.first << " " << t.second<<std::endl;
            list_server.push_back(t.second);
        }
        delete response_buffer;
    }
    return list_server;
}

std::vector<std::string> KVstore::list_rowkeys(){
    std::string empty = "";
    sockaddr_in master_sock_addr = get_address(master_ip_str);
    int sockfd = socket(PF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) 
    {
        std::cerr<<"Unable to create socket"<<std::endl;

    }
    connect(sockfd, (struct sockaddr*)& master_sock_addr, sizeof(master_sock_addr));
    
    std::vector<std::string> rkey_initial;
    std::pair<std::string, std::string> response_str;
    std::vector<std::string> row_keys_vec;
    rkey_initial.push_back("0");
    rkey_initial.push_back("a");
    rkey_initial.push_back("z");
    for(int rkey = 0; rkey < rkey_initial.size();rkey ++){
        std::pair<std::string, std::string> result = process_kvstore_request("LIST_ROWKEY", rkey_initial[rkey], empty, empty, empty);
        PennCloud::Response response;
        response.ParseFromString(result.first);
        std::copy(response.row_keys().begin(), response.row_keys().end(), std::back_inserter(row_keys_vec));
    }
    return row_keys_vec;
}

std::vector<std::string> KVstore::list_colkeys(const std::string &rkey){
    // if rkey in cache, directly send it to storage server (if storage server cannot be connected, recontact master)
    std::string empty = "";
    std::vector<std::string> col_keys_vec;
    std::pair<std::string, std::string> result = process_kvstore_request("LIST_COLKEY", rkey, empty, empty, empty);
    PennCloud::Response response;
    response.ParseFromString(result.first);
    std::copy(response.col_keys().begin(), response.col_keys().end(), std::back_inserter(col_keys_vec));
    sort(col_keys_vec.begin(), col_keys_vec.end(), std::greater<std::string>());
    return col_keys_vec;
}
std::string KVstore::storage_create(const std::string &rkey, const std::string &parent, const std::string &name, bool is_dir)
{
    std::string value2 = std::to_string(int(is_dir));
    std::pair<std::string, std::string> result = process_kvstore_request("CREATE", rkey, parent, name, value2);
    return result.first;

}

std::string KVstore::storage_rename(const std::string &rkey, const std::string &parent, const std::string &name, const std::string &target2)
{
    std::pair<std::string, std::string> result = process_kvstore_request("RENAME", rkey, parent, name, target2);
    return result.first;
}
// Sample Test
int main()
{
    KVstore kv_test;
    //Storage create testing
    // std::string response_str;
    // response_str = kv_test.storage_create("abc", "cab", "abc", true);
    // kv_test.put("abc", "password", "frontend");
    // response_str = kv_test.storage_create("abc", "cab", "abc", true);
    // kv_test.put("abc", "cab", "frontend");
    // response_str = kv_test.storage_create("abc", "cab", "abc", true);
    // kv_test.put("abc", "cab", "/abc/inode:");
    // response_str = kv_test.storage_create("abc", "cab", "inode", true);
    // response_str = kv_test.storage_create("abc", "cab", "def", true); //abc-c -> 1, content = "def:1:d/", abc-cab -> content, abc-1 -> "/" return 1
    // std::cout<<response_str;

    //Storage rename testing
    std::string response_str;
    response_str = kv_test.storage_rename("abc", "cab", "abc", "Def");
    kv_test.put("abc", "password", "frontend");
    response_str = kv_test.storage_rename("abc", "cab", "abc", "Def");
    kv_test.put("abc", "cab", "frontend");
    response_str = kv_test.storage_rename("abc", "cab", "abc", "Def");
    kv_test.put("abc", "cab", "/abc:");
    response_str = kv_test.storage_rename("abc", "cab", "inode", "Def");
    kv_test.put("abc", "cab", "/abc/inode:");
    response_str = kv_test.storage_rename("abc", "cab", "inode", "Def");
    response_str = kv_test.storage_rename("abc", "cab", "def", "Def"); //abc-c -> 1, content = "def:1:d/", abc-cab -> content, abc-1 -> "/" return 1
    std::cout<<response_str;

    // kv_test.put("abccc", "password", "frontend");
    // kv_test.put("abccc", "bb", "frontend");
    // kv_test.put("abccc", "zz", "frontend");
    // kv_test.put("abccc", "aa", "frontend");
    // kv_test.list_colkeys("abccc");

    // std::vector<bool> test_map = kv_test.list_server_status();
    // kv_test.resurrect(0);
    // kv_test.kill(3);
    // kv_test.kill(6);
    // kv_test.kill(1);
    // kv_test.kill(2);
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