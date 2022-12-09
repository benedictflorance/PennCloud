#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <iostream>
#include <csignal>
#include <fstream>
#include <sys/socket.h>
#include <vector>
#include <cstring>
#include <string>
#include <pthread.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <dirent.h>
#include <unordered_set>
#include <unordered_map>
#include <string>
#include <algorithm>
#include <chrono>
#include <ctime> 
#include <sys/file.h>
#include <sstream>
#include <thread>
#include <filesystem>
#include <sys/time.h>
#include "../request.pb.h"
#include "../response.pb.h"

namespace fs = std::filesystem;
using namespace std;

// Message constants
const char* invalid_ip_message = "-ERR Invalid IP/port argument. Please adhere to <IP Address>:<Port Number>\r\n";
const char* shutdown_message = "-ERR Tablet server shutting down\r\n";
const char* new_connection_message = "New connection\r\n";
const char* closing_message = "Connection closed\r\n";
pair<const char*, const char*> service_ready_message = make_pair("+OK", "Tablet server ready");
pair<const char*, const char*> type_unset_message = make_pair("-ERR", "Request type not set");
pair<const char*, const char*>  unrecognized_command_message = make_pair("-ERR", "Unrecognized command");
pair<const char*, const char*>  param_unset_message = make_pair("-ERR", "Parameter(s) required for this command are not set");
pair<const char*, const char*>  invalid_rowkey_message = make_pair("-ERR", "This tablet does not process this rowkey");
pair<const char*, const char*>  key_inexistence_message = make_pair("-ERR", "Row or column key doesn't exist");

// Integer constants
const int LENGTH_BUFFER_SIZE = 20; 
const int BUFFER_SIZE = 15000000;
const int RKEY_BUFFER_SIZE = 10000;
const int CKEY_BUFFER_SIZE = 25000;

const string replicas_header = "<REPLICAS>";
const string checkpt_dir = "checkpoints/";
const string checkpt_meta_dir = "metadata/";
const string log_dir = "log/";
const string meta_log_dir = "metadata_log/";

// Global variables
bool verbose = false;
volatile bool shutdown_flag = false;
string config_file;
int curr_server_index;
int server_socket;
vector<sockaddr_in> tablet_addresses;
string curr_ip_addr;
vector<int> rowkey_range;
sockaddr_in master_address;
vector<int> client_sockets;
vector<pthread_t> client_threads;
unordered_map<string, unordered_map<string, string> > kv_store;
unordered_map<int, int> rkey_to_primary;
unordered_map<int, vector<int> > tablet_server_group;
bool isPrimary = false;
int socket_to_master;
string log_file_name;
string meta_log_file_name;

//global vars for replication
const char* alive_command = "ALIVE";
const char* write_command = "WRITE:";
const char* ack_command= "ACK:";
const char* grant_command = "GRANT:";
//map for each primary to store no of acks for each request
unordered_map<string,int> number_of_acks;
unordered_map<string, int> req_client_sock_map;
mutex rowkeymaplock;
map<string, mutex> rowkey_lock;
unordered_map<string, int> rowkey_version;
map<string, mutex> rowkey_version_lock;
// Holdback queue: string is rkey, int is seqno and string is request_str
unordered_map<string, map<int, string>> replica_holdback_queue;
