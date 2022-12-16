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
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <functional>
#include "utils/stdc++.h"
#include <ctime>
#include "utils/update_manager.h"
#include "utils/date/date.h"
#include <chrono>
#include "request.pb.h"
#include "response.pb.h"

using namespace std;
#include "utils/hash.h"
#include "utils/MD5.h"

// boolean for the v flag (debugging)
bool v = false;

int NUM_SERVERS = 9;

pthread_t id_heartbeat;
pthread_t id_frontend;

string config_file = "configs/tablet_server_config.txt";
string master_address;

//vector of pair of rowkeyrange: start, end, vector of tablets supporting it
vector<pair<int,pair<int,vector<int> > > > rowkey_range;
vector<pair<int,pair<int,vector<int> > > > original_rowkey_range;

//vector of tablet addresses
vector<string> tablet_addresses;

//map for server status
unordered_map<int,bool> server_status;

//struct for heartbeats of each tablet server
typedef struct Heartbeat{
  int counter;
  int server_index;
  string status = "ALIVE";
  long long timestamp;
}Heartbeat;

//mapping from tablet server address to heartbeat
unordered_map<string, Heartbeat > heartbeat;

//primary info  kvst
unordered_map<int, int> rkey_to_primary;
unordered_map<int, vector<int> > tablet_server_group;
sockaddr_in get_address(string socket_address)
{
    int colon_index = socket_address.find(":");
    string ip_address = socket_address.substr(0, colon_index);
    string port_str = socket_address.substr(colon_index + 1, socket_address.length() - colon_index - 1);
    if(ip_address.empty())
    {
            std::cout << ip_address << " " << port_str << std::endl;
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
void initialize_primary_info(string config_file)
{
    ifstream config_fstream(config_file);
    string line;
    getline(config_fstream, line); // Get <MASTER>
    getline(config_fstream, line);
    getline(config_fstream, line); // Get <TABLETS>
    int index = 0;
    while(getline(config_fstream, line))
    {
        if(line == "<REPLICAS>")
            break;
    }
    while(getline(config_fstream, line))
    {
        stringstream replica(line);
        string row_range;
        getline(replica, row_range, ',');
        vector<int> server_indices;
        string index;
        while(getline(replica, index, ','))
            server_indices.push_back(stoi(index));
        int start, end;
        int hyphen_index = row_range.find("-");
        start = row_range[0];
        end  = row_range[2];
        rkey_to_primary[toKey(start, end)] = server_indices[0];
        tablet_server_group[toKey(start, end)] = server_indices;
        if(v)
        {
            cout<<"Rowkey range "<<(char) start<<" to "<<(char) end<<" has the key as "<<toKey(start, end)
                <<" and primary server "<<rkey_to_primary[toKey(start, end)]<<" and tablet servers :";
            for(int i = 0; i < tablet_server_group[toKey(start, end)].size(); i++)
                cout<<tablet_server_group[toKey(start, end)][i]<<" ";
            cout<<endl;
        }
    }
}

// function for reading
bool do_read(int fd, char *buf, int len){
  int rcvd = 0;
  while (rcvd < len)
  {
    int n = read(fd, &buf[rcvd], len - rcvd);
    cout << n << endl;
    if (n < 0)
      return false;
    rcvd += n;
  }
  return true;
}

// function for writing
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

// convert string to lowercase char by char
char to_lowercase(char c){
  if (c >= 'A' && c <= 'Z')
  {
    return c + 32;
  }

  return c;
}

//store rowkey to tablet server mapping an populate master
void process_tablet_file(){
    ifstream config_fstream(config_file);
    string line;
    getline(config_fstream, line); // Get <MASTER>
    getline(config_fstream, line);
    std::size_t pos;
    pos = line.find("\r"); if (pos != std::string::npos) line.replace(pos, 1, "");
    master_address = line;
    getline(config_fstream, line); // Get <TABLETS>
    int index = 0;
    while(getline(config_fstream, line))
    {
        pos = line.find("\r"); if (pos != std::string::npos) line.replace(pos, 1, "");
        if(line == "<REPLICAS>")
            break;
        tablet_addresses.push_back(line);
        index++;
    }
    while(getline(config_fstream, line))
    {
        pos = line.find("\r"); if (pos != std::string::npos) line.replace(pos, 1, "");
        stringstream replica(line);
        string row_range;
        getline(replica, row_range, ',');
        vector<int> server_indices;
        string index;
        while(getline(replica, index, ','))
            server_indices.push_back(stoi(index));
        int start, end;
        int hyphen_index = row_range.find("-");
        start = row_range[0];
        end  = row_range[2];
        rowkey_range.push_back(make_pair(start, make_pair(end,server_indices)));
        original_rowkey_range.push_back(make_pair(start, make_pair(end,server_indices)));
    }
}

//assign new primary
void assign_new_primary(int start, int end, string tablet_server_index){
  for(int i = 0; i < tablet_server_group[toKey(start, end)].size(); i++) // 1, 2
  {
      int socket_to_server = socket(PF_INET, SOCK_STREAM, 0);
      if (socket_to_server < 0) 
      {
          if(v)
              cerr<<"Unable to create socket"<<endl;
      }
      sockaddr_in serv_addr = get_address(tablet_addresses[tablet_server_group[toKey(start, end)][i]]);
      if(connect(socket_to_server, (struct sockaddr*)& serv_addr, 
              sizeof(serv_addr)) < 0)
      {
          cerr<<"Connect failed while contacting server for new primary"<<endl;
          continue;
      }
      char req_length[11];
      string request_str;
      PennCloud::Request request;
      request.set_type("NEW_PRIMARY");
      request.set_isserver("true");
      request.set_new_primary_index(to_string(rkey_to_primary[toKey(start, end)])); // 1
      request.set_modified_server_index(tablet_server_index); // 2
      request.SerializeToString(&request_str);
      memset(req_length, 0, sizeof(req_length));
      snprintf (req_length, 11, "%10d", request_str.length()); 
      write(socket_to_server, string(req_length).c_str(), string(req_length).length());      
      int return_val = write(socket_to_server, request_str.c_str(), strlen(request_str.c_str()));
      cerr<<"Master is telling "<<tablet_server_group[toKey(start, end)][i]<<"about new primary "<<endl;
      close(socket_to_server);
  }
}

// resurrect a server
void resurrect_server(int start, int end, string tablet_server_index){ 
  for(int i = 0; i < tablet_server_group[toKey(start, end)].size(); i++) // 1, 2
  {
      int socket_to_server = socket(PF_INET, SOCK_STREAM, 0);
      if (socket_to_server < 0) 
      {
          if(v)
              cerr<<"Unable to create socket"<<endl;
      }
      sockaddr_in serv_addr = get_address(tablet_addresses[tablet_server_group[toKey(start, end)][i]]);
      if(connect(socket_to_server, (struct sockaddr*)& serv_addr, 
              sizeof(serv_addr)) < 0)
      {
          cerr<<"Connect failed while contacting server for new primary"<<endl;
          continue;
      }
      char req_length[11];
      string request_str;
      PennCloud::Request request;
      request.set_type("RESURRECT");
      request.set_isserver("true");
      request.set_modified_server_index(tablet_server_index); 
      request.SerializeToString(&request_str);
      memset(req_length, 0, sizeof(req_length));
      snprintf (req_length, 11, "%10d", request_str.length()); 
      write(socket_to_server, string(req_length).c_str(), string(req_length).length());      
      int return_val = write(socket_to_server, request_str.c_str(), strlen(request_str.c_str()));
      cerr<<"Master is telling "<<tablet_server_group[toKey(start, end)][i]<<"about a resurrected server "<<tablet_server_index<<endl;
      close(socket_to_server);
  }
}
// worker function to handle each client in a separate thread
void worker(int comm_fd,struct sockaddr_in clientaddr){

  size_t bytes_read;
  string crlf = "\r\n";
  char buffer[100000];
  int msg_size = 0;
  string left_over="";
  string req = "req";
  string alive ="alive";
  string kill = "kill";
  string resurrect = "resurrect";
  string status = "status";

  if(v){
    fprintf(stderr, "[%d] New Connection\n", comm_fd);
  }

  while (true){
    bzero(buffer, sizeof(buffer));
    msg_size=0;

    if(left_over.find(crlf)!= string::npos){
      strcpy(buffer, left_over.c_str()); 
    }
    else{
      while ((bytes_read = read(comm_fd, buffer + msg_size, sizeof(buffer) - msg_size - 1)) > 0)
      {
          msg_size += bytes_read;
          string temp(buffer);
          // if \r\n are at the end
          if (buffer[msg_size - 2] == '\r' && buffer[msg_size - 1] == '\n')
            break;

          // if \r\n are in the middle
          if (temp.find(crlf) != string::npos){
            // found the end of one command
            break;
          }
      }
    }
    if(bytes_read == 0)
    {
 			close(comm_fd);
			pthread_exit(NULL);     
    }
    // convert buffer to lowercase
    string lower_case(buffer);
    string append = left_over + lower_case;
    lower_case = append;
    // if(v){
    //   fprintf(stderr, "[%d] C: %s", comm_fd,buffer);
    // }
    for (char &c : lower_case){
      c = to_lowercase(c);
    }
    if (lower_case.find(req) != string::npos){
      // capture the string before \r\n and extract argument
      string argument(buffer);
      string append = left_over + argument;
      argument = append;
      //find index of \r\n
      int found = argument.find(crlf);
      argument = argument.substr(0, found);

      // erasing "req" from the string
      argument.erase(0, 3);

      //now find the row key
      string row_key;
      int start = argument.find('(');
      int end = argument.find(')');
      
      row_key = argument.substr(start+1,end-1);
      int initial = (int)(compute_hash(row_key)[0]);
      cout<<"Initial: "<<char(initial)<<endl;

      for(auto nested_pair: rowkey_range){
        pair<int, vector<int> > pair = nested_pair.second;
        if(nested_pair.first<= initial && pair.first>=initial){
            if(v){
                cout<<"Found appropriate tablet servers"<<endl;
            }

            //SEG FAULT HERE:
            //now pick a random server
            vector<int> row_key_tablet_server = pair.second;
            int tablet_servers_size = row_key_tablet_server.size();
            if(tablet_servers_size!=0){
              //generate a random index from the list of servers
              int index = (int)(rand() % (tablet_servers_size));
              string tablet_address = tablet_addresses[row_key_tablet_server[index]];
              if(v){
                  fprintf(stderr, "[%d] Generated Index Is: %d \n", comm_fd,index);
                  fprintf(stderr, "[%d] Selected Tablet Server Is: %s \n", comm_fd,tablet_address.c_str());
              }

              do_write(comm_fd, (char *)tablet_address.c_str(), tablet_address.size());

              if(v){
                  fprintf(stderr, "[%d] S: %s", comm_fd,tablet_address.c_str());
              }
              break;

            }
            else{
              //catch exception
              string servers_dead = "DEAD";
              cout<<servers_dead<<endl;
              do_write(comm_fd, (char *)servers_dead.c_str(), servers_dead.size());
            }
            
        }
      }

    }
    else if (lower_case.find(kill)!=string::npos){
      // capture the string before \r\n and extract argument
      string argument(buffer);
      string append = left_over + argument;
      argument = append;
      //find index of \r\n
      int found = argument.find(crlf);
      argument = argument.substr(0, found);

      // erasing "kill" from the string
      argument.erase(0, 4);

      //now find the row key
      string row_key;
      int bracket_start = argument.find('(');
      int bracket_end = argument.find(')');
      
      string tablet_server_index = argument.substr(bracket_start+1,bracket_end-1);
      cout<<"Received kill for "<<tablet_server_index<<endl;
      if(server_status[stoi(tablet_server_index)])
      {
        server_status[stoi(tablet_server_index)] = false;
        int start, end;
        bool primary_crash = false;
        for(auto &nested_pair: rowkey_range){
          vector<int> &current_group =   nested_pair.second.second;
          auto it = find(current_group.begin(),current_group.end(),stoi(tablet_server_index));
          if(it!=current_group.end()){
            start = nested_pair.first;
            end = nested_pair.second.first;
            current_group.erase(it);
            if(rkey_to_primary[toKey(start, end)] == stoi(tablet_server_index)){
                rkey_to_primary[toKey(start, end)] = current_group[0]; 
                primary_crash = true;
            }
          }
        }
        assign_new_primary(start, end, tablet_server_index);
      }
    }
    else if (lower_case.find(resurrect)!=string::npos){
      // capture the string before \r\n and extract argument
      string argument(buffer);
      string append = left_over + argument;
      argument = append;
      //find index of \r\n
      int found = argument.find(crlf);
      argument = argument.substr(0, found);

      // erasing "resurrect" from the string
      argument.erase(0, 9);

      //now find the row key
      string row_key;
      int bracket_start = argument.find('(');
      int bracket_end = argument.find(')');
      cout<<"Argument start and end" << bracket_start<<" "<<bracket_end<<endl;
      
      string tablet_server_index = argument.substr(bracket_start+1,bracket_end-1);
      cout<<"Received unkill for "<<tablet_server_index<<endl;
      if(!server_status[stoi(tablet_server_index)])
      {
        server_status[stoi(tablet_server_index)] = true;

        vector<pair<int, int>> matching_rowkeys;
        int start, end;
        for(auto &nested_pair: original_rowkey_range){
          vector<int> &current_group =   nested_pair.second.second;
          auto it = find(current_group.begin(),current_group.end(), stoi(tablet_server_index));
          if(it != current_group.end()){
            start = nested_pair.first;
            end = nested_pair.second.first;
            matching_rowkeys.push_back(make_pair(nested_pair.first, nested_pair.second.first));
          }
        }
        for(auto &nested_pair: rowkey_range){
          vector<int> &current_group =   nested_pair.second.second;
          auto it = find(current_group.begin(),current_group.end(), stoi(tablet_server_index));
          int rkey_start = nested_pair.first, rkey_end = nested_pair.second.first;
          if(find(matching_rowkeys.begin(), matching_rowkeys.end(), pair<int, int> (rkey_start, rkey_end)) != matching_rowkeys.end())
          {
            current_group.push_back(stoi(tablet_server_index));
          }

        }      
        resurrect_server(start, end, tablet_server_index);        
      }
    }
    else if(lower_case.find(alive) != string::npos){
        //check which server it is
        string argument(buffer);
        string append = left_over + argument;
        argument = append;

        //find index of hyphen
        int found = argument.find('-');
        argument = argument.substr(found+1);
        string str = tablet_addresses[stoi(argument)];

        //if it is the first heartbeat from the tablet server
        if(heartbeat.find(str)==heartbeat.end()){
            Heartbeat temp_h;
            temp_h.server_index = stoi(argument);
            temp_h.counter =0;
            temp_h.status = "ALIVE";
            heartbeat.insert({str,temp_h});     
        }
        //get the time heartbeat was received
        time_t timeInSec;
        time(&timeInSec);
        //increment the heartbeat counter
        heartbeat[str].counter++;
        heartbeat[str].timestamp =(long long) timeInSec;
        // if(v)
        // {
        //   cerr<<"Heartbeat received from "<<str<<" with counter "<<heartbeat[str].counter<<" and timestamp "<<heartbeat[str].timestamp<<endl;
        // }
    }
    else if(lower_case.find(status)!=string::npos){

        string response_str;
        PennCloud::Response response;
        for(auto item : server_status)
        {
            (*response.mutable_server_status())[item.first] = item.second;
        }
        response.set_status("+OK");
        response.SerializeToString(&response_str);
        char req_length[11];
        snprintf (req_length, 11, "%10d", response_str.length()); 
        std::string message = std::string(req_length) + response_str;
        do_write(comm_fd, message.data(), message.length());

    }
    else{
      char unknown[] = "-ERR Unknown command\r\n";
      do_write(comm_fd, unknown, sizeof(unknown)-1);
      if(v){
        fprintf(stderr, "[%d] S: %s", comm_fd,unknown);
      }
    }

    // find buffer minus the command we processed
    string buffer_string(buffer);
    int found = buffer_string.find(crlf);
    if(found!= string::npos){
      buffer_string = buffer_string.substr(found + crlf.size());
    }
    left_over = buffer_string;
  }

  close(comm_fd);
  if(v){
      fprintf(stderr, "[%d] Connection Closed\n", comm_fd);
  }
  pthread_exit(NULL);
}

// function for creating server
void createServer(){
  // Socket Creation
  int listen_fd = socket(PF_INET, SOCK_STREAM, 0);
  // check socket to make it non blocking
  if (listen_fd < 0){
    if(v)
      fprintf(stderr, "Cannot open socket (%s)\n", strerror(errno));
    exit(2);
  }

  struct sockaddr_in servaddr;
  bzero(&servaddr, sizeof(servaddr));
  servaddr.sin_family = AF_INET;
  //bind on the master address as given in the config file
  inet_pton(AF_INET, master_address.c_str(), &(servaddr.sin_addr));
  //servaddr.sin_addr.s_addr = htons(INADDR_ANY);
  if(v)
    cout<<"Listening on "<<master_address<<endl;
  int colon_index = master_address.find(':');
  string port_str = master_address.substr(colon_index + 1);
  int port;
  try{
        port = stoi(port_str);
  }
  catch(const std::invalid_argument&)
    {
        cerr <<"Invalid IP Address \n";
        exit(-1);
  }
  servaddr.sin_port = htons(port);

  // Bind
  // check bind to make it non blocking
  if(::bind(listen_fd, (struct sockaddr *)&servaddr, sizeof(servaddr))<0){
    if(v)
      fprintf(stderr, "Bind error (%s)\n", strerror(errno));
    exit(2);
  }

  // Listen
  // the second parameter corresponds to backlog in listening
  int l = listen(listen_fd, SOMAXCONN);
  // check listen to make it non blocking
  if(l<0){
    if(v)
      fprintf(stderr, "Cannot listen port (%s)\n", strerror(errno));
    exit(2);
  }

  // Multithreaded server
  while (true){
    struct sockaddr_in clientaddr;
    socklen_t clientaddrlen = sizeof(clientaddr);

    // accept incoming connections
    int fd = accept(listen_fd, (struct sockaddr *)&clientaddr, &clientaddrlen);
    // check accept to make it non blocking
    if(fd<1){
      if(v)
        fprintf(stderr, "Accept fails(%s)\n", strerror(errno));
      exit(2);
    }

    // write the greeting
    // char greeting[] = "+OK Master Server ready (Author: Namita Shukla / nashukla) \r\n";
    // do_write(fd, greeting, sizeof(greeting) - 1);

    if(v)
        fprintf(stderr, "Creating handler for frontend\n");
    thread handle_frontend(worker,fd,clientaddr);
    id_frontend = handle_frontend.native_handle();
    handle_frontend.detach();
  }
}

//check if all servers are alive 
void alive(){
    while(true){
        auto t = UpdateManager::start();
        this_thread::sleep_for(10s);

        // if(v){
        //     cout<<"Checking for server status "<<endl;
        // }
        time_t timeInSec;
        time(&timeInSec);
        int threshold = 4;
        for(auto h: heartbeat){
            Heartbeat current_h = h.second;
            if((timeInSec - current_h.timestamp) > threshold){
                if(v){
                    cout<<"DEAD server detected "<<h.first<<endl;
                }
                server_status[current_h.server_index] = false;
                h.second.status = "DEAD";
            }
            else{
                h.second.status = "ALIVE";
                server_status[current_h.server_index] = true;
            }
        }
    }   
}

//kill all the running threads and exit
void signal_handler(int arg){
    pthread_cancel(id_heartbeat);
    pthread_cancel(id_frontend);
    exit(0);
}

int main(int argc, char *argv[]){

  signal(SIGINT, signal_handler);

  // boolean for the a flag
  bool a = false;

  int c;
  while ((c = getopt(argc, argv, "av")) != -1)
    switch (c)
    {
    case 'a':
      a = true;
      break;
    case 'v':
      v = true;
      break;
    }

  if (a){
    fprintf(stderr, "%s", "Namita Shukla (nashukla) \n");
    exit(1);
  }

  //process file
  process_tablet_file();

  initialize_primary_info(config_file);

  for(int i = 0; i < NUM_SERVERS; i++)
  {
      server_status[i] = true;
  }

  //start evaluating server status
  thread handle_alive(alive);
  id_heartbeat = handle_alive.native_handle();

  // create listening server
  createServer();

  return 0;
}
