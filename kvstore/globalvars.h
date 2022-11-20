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
using namespace std;

// Message constants
const char* invalid_ip_message = "-ERR Invalid IP/port argument. Please adhere to <IP Address>:<Port Number>\r\n";
const char* shutdown_message = "-ERR Tablet server shutting down\r\n";
const char* service_ready_message = "+OK Tablet server ready\r\n";
const char* new_connection_message = "New connection\r\n";
const char* closing_message = "Connection closed\r\n";

// Integer constants
const int BUFFER_SIZE = 50000;
const int suffix_length = strlen("\r\n");

// Global variables
bool verbose = false;
volatile bool shutdown_flag = false;
string config_file;
int curr_server_index;
vector<sockaddr_in> tablet_addresses;
vector<pair<int, int>> rowkey_range;
sockaddr_in master_address;
int server_socket;
vector<int> client_sockets;
vector<pthread_t> client_threads;