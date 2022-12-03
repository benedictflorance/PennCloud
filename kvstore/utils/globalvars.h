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
#include "../request.pb.h"
#include "../response.pb.h"
namespace fs = std::filesystem;
using namespace std;

// Message constants
const char* invalid_ip_message = "-ERR Invalid IP/port argument. Please adhere to <IP Address>:<Port Number>\r\n";
const char* shutdown_message = "-ERR Tablet server shutting down\r\n";
const char* new_connection_message = "New connection\r\n";
const char* closing_message = "Connection closed\r\n";
const char* alive_command = "ALIVE";
pair<const char*, const char*> service_ready_message = make_pair("+OK", "Tablet server ready");
pair<const char*, const char*> type_unset_message = make_pair("-ERR", "Request type not set");
pair<const char*, const char*>  unrecognized_command_message = make_pair("-ERR", "Unrecognized command");
pair<const char*, const char*>  param_unset_message = make_pair("-ERR", "Parameter(s) required for this command are not set");
pair<const char*, const char*>  invalid_rowkey_message = make_pair("-ERR", "This tablet does not process this rowkey");
pair<const char*, const char*>  key_inexistence_message = make_pair("-ERR", "Row or column key doesn't exist");

// Integer constants
const int BUFFER_SIZE = 500000;
const int suffix_length = strlen("\r\n");
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
vector<sockaddr_in> tablet_addresses;
string curr_ip_addr;
vector<pair<int, int>> rowkey_range;
sockaddr_in master_address;
int server_socket;
vector<int> client_sockets;
vector<pthread_t> client_threads;
unordered_map<string, unordered_map<string, string> > kv_store;
unordered_map<int, int> rkey_to_primary;
unordered_map<int, vector<int> > tablet_server_group;
bool isPrimary = false;
int socket_to_master;
std::mutex kvstore_lock;
string log_file_name;
string meta_log_file_name;