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
#include "include/stdc++.h"
#include <ctime>
#include "include/update_manager.h"
#include "include/date/date.h"
#include <chrono>

using namespace std;

// boolean for the v flag (debugging)
bool v = false;

string config_file = "tablet_server_config.txt";
string master_address;

//vector of pair of rowkeyrange: start, end, vector of tablets supporting it
vector<pair<int,pair<int,vector<int> > > > rowkey_range;

//vector of tablet addresses
vector<string> tablet_addresses;

//struct for heartbeats of each tablet server
typedef struct Heartbeat{
  int counter;
  string status = "ALIVE";
}Heartbeat;

//mapping from tablet server address to heartbeat
unordered_map<string, Heartbeat > heartbeat;

//counter for number of alive commands received
long long no_of_alive;

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
void process_tablet_file()
{
    ifstream config_fstream(config_file);
    string line;
    getline(config_fstream, line); // Get <MASTER>
    getline(config_fstream, line);
    master_address = line;
    getline(config_fstream, line); // Get <TABLETS>
    int index = 0;
    while(getline(config_fstream, line))
    {
        if(line == "<REPLICAS>")
            break;
        tablet_addresses.push_back(line);
        index++;
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
        rowkey_range.push_back(make_pair(start, make_pair(end,server_indices)));
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
  string alive;

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

    // convert buffer to lowercase
    string lower_case(buffer);
    string append = left_over + lower_case;
    lower_case = append;
    if(v){
      fprintf(stderr, "[%d] C: %s", comm_fd,buffer);
    }
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
      int initial = (int)row_key[0];

      for(auto nested_pair: rowkey_range){
        pair<int, vector<int> > pair = nested_pair.second;
        if(nested_pair.first<= initial && pair.first>=initial){
            if(v){
                cout<<"Found appropriate tablet servers"<<endl;
            }
            //now pick a random server
            vector<int> row_key_tablet_server = pair.second;
            int tablet_servers_size = row_key_tablet_server.size();
            //generate a random index from the list of servers
            int index = (int)(rand() % (tablet_servers_size));
            string tablet_address = tablet_addresses[row_key_tablet_server[index]] + crlf;
            if(v){
                fprintf(stderr, "[%d] Generated Index Is: %d", comm_fd,index);
                fprintf(stderr, "[%d] Selected Tablet Server Is: %s", comm_fd,tablet_address.c_str());
            }

            do_write(comm_fd, (char *)tablet_address.c_str(), tablet_address.size());

            if(v){
                fprintf(stderr, "[%d] S: %s", comm_fd,tablet_address.c_str());
            }
            break;
        }
      }

    }
    else if(lower_case.find(alive) != string::npos){
        //increment alive counter
        no_of_alive++;
        //check which server it is
        socklen_t clientaddrlen = sizeof(clientaddr);
        char str[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &(clientaddr), str, INET_ADDRSTRLEN);
        if(heartbeat.find(str)==heartbeat.end()){
            heartbeat[str].counter = 0;
        }
        heartbeat[str].counter++;
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
void createServer(int p){
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
  servaddr.sin_port = htons(p);

  // Bind
  bind(listen_fd, (struct sockaddr *)&servaddr, sizeof(servaddr));

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
    char greeting[] = "+OK Master Server ready (Author: Namita Shukla / nashukla) \r\n";
    do_write(fd, greeting, sizeof(greeting) - 1);

    if(v)
        fprintf(stderr, "Creating handler for frontend\n");
    thread handle_frontend(worker,fd,clientaddr);
    handle_frontend.join();
  }
  close(listen_fd);
}

//check if all servers are alive 
void alive(){
    while(true){
        auto t = UpdateManager::start();
        this_thread::sleep_for(10s);

        if(v){
            cout<<"Checking for server status"<<endl;
        }
        for(auto h: heartbeat){
            int diff = no_of_alive % (tablet_addresses.size());
            Heartbeat current_h = h.second;
            if(abs(diff - current_h.counter) < 2){
                if(v){
                    cout<<"DEAD server detected "<<h.first<<endl;
                }
                h.second.status = "DEAD";
            }
            else{
                h.second.status = "ALIVE";
            }
        }
    }   
}

int main(int argc, char *argv[]){
  // storing the port number, default is 10000
  int p = 10000;

  // boolean for the a flag
  bool a = false;

  int c;
  while ((c = getopt(argc, argv, "avp:")) != -1)
    switch (c)
    {
    case 'a':
      a = true;
      break;
    case 'p':
      p = stoi(optarg);
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
  thread handle_alive(alive);
  

  // cout<<master_address<<endl;
  // for(auto pair : rowkey_range){
  //   cout<<(char) pair.first<<"-"<< (char) pair.second.first<<" "<<endl;
  //   for(auto i : pair.second.second)
  //       cout<<i<<endl;
  // }

  // create listening server
  createServer(p);

  return 0;
}
