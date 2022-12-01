#include "utils/globalvars.h"
#include "utils/hash.h"
#include "utils/config_processor.h"
#include "request.pb.h"
#include "response.pb.h"
string master_ip_str = "127.0.0.1:8000"; 

class KVstore
{
    private:
    unordered_map<string, sockaddr_in> rkey_to_storage_cache;
    pair<string, string> send_request(int sockfd, string type, string rkey, string ckey, string value1, string value2);
    pair<string, string> contact_tablet_server(string type, string rkey, string ckey, string value1, string value2);
    pair<string, string> process_kvstore_request(string type, string rkey, string ckey, string value1="", string value2 = "");
    public:
    string get(string rkey, string ckey) {
        return process_kvstore_request("get", rkey, ckey).first;
    }
    void put(string rkey, string ckey, string value) {
        process_kvstore_request("put", rkey, ckey, value);
    }
    bool del(string rkey, string ckey);
    bool cput(string rkey, string ckey, string value1, string value2);
};

pair<string, string> KVstore::send_request(int sockfd, string type, string rkey, string ckey, string value1, string value2)
{
    string request_str;
    char response_buffer[BUFFER_SIZE];
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
    request_str += "\r\n";
    write(sockfd, request_str.c_str(), strlen(request_str.c_str()));
    while(read(sockfd, response_buffer, BUFFER_SIZE) == 0);
    response.ParseFromString(response_buffer);
    pair<string, string> response_str = make_pair(response.value(), response.status());
    return response_str;
}
pair<string, string> KVstore::contact_tablet_server(string type, string rkey, string ckey, string value1, string value2)
{
    int sockfd = socket(PF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) 
    {
        if(verbose)
            cerr<<"Unable to create socket"<<endl;
        return pair<string, string>("", "-CRASH");

    }
    if(connect(sockfd, (struct sockaddr*)& rkey_to_storage_cache[rkey], sizeof(rkey_to_storage_cache[rkey])) < 0)
    {
        // Tablet server crash
        rkey_to_storage_cache.erase(rkey);
        // Contact master again!
        close(sockfd);
        return pair<string, string>("", "-CRASH");
    }
    // Send request here
    pair<string, string> response_str = send_request(sockfd, type, rkey, ckey, value1, value2);
    close(sockfd);
    return response_str;
}
// clear cache every 2 minutes
pair<string, string> KVstore::process_kvstore_request(string type, string rkey, string ckey, string value1, string value2)
{
    bool is_crash = false;
    pair<string, string> response_str;
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
                if(verbose)
                    cerr<<"Unable to create socket"<<endl;
                return pair<string, string>("", "-MASTERCRASH");

            }
            connect(sockfd, (struct sockaddr*)& master_sock_addr, sizeof(master_sock_addr));
            // Send request here
            string rkey_request_msg = "REQ(" + rkey + ")\r\n";
            char response_buffer[BUFFER_SIZE];
            write(sockfd, rkey_request_msg.c_str(), strlen(rkey_request_msg.c_str()));
            while(read(sockfd, response_buffer, BUFFER_SIZE) == 0);
            string tablet_ip_str = string(response_buffer);
            // Cache it 
            cout<<tablet_ip_str<<endl;
            rkey_to_storage_cache[rkey] = get_address(tablet_ip_str);
            response_str = contact_tablet_server(type, rkey, ckey, value1, value2);
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

// Sample Test
int main()
{
    KVstore kv_test;
    pair<string, string> response_str = kv_test.process_kvstore_request("PUT", "10hanbang", "password", "frontend");
    cout<<response_str.first<<" "<<response_str.second<<endl;
    response_str = kv_test.process_kvstore_request("GET", "10hanbang", "password");
    cout<<response_str.first<<" "<<response_str.second<<endl;
    response_str = kv_test.process_kvstore_request("CPUT", "10hanbang", "password", "frontend", "backend");
    cout<<response_str.first<<" "<<response_str.second<<endl;
    response_str = kv_test.process_kvstore_request("GET", "10hanbang", "password");
    cout<<response_str.first<<" "<<response_str.second<<endl;
    response_str = kv_test.process_kvstore_request("DELETE", "10hanbang", "password");
    cout<<response_str.first<<" "<<response_str.second<<endl;
    response_str = kv_test.process_kvstore_request("GET", "10hanbang", "password");
    cout<<response_str.first<<" "<<response_str.second<<endl;
}