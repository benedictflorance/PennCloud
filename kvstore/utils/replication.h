bool replica_msg_check(sockaddr_in clientaddr);
string get_time();
vector<int> find_my_replica_group(string request_str);
pair<int,int> find_rowkey_range(string request_str);
string update_kv_store(string request_str, int client_socket = -1);
void update_secondary(string request_str);
void grant_secondary(string request_str);
void request_primary(string request_str);
bool  do_write(int fd, char *buf, int len);

bool replica_msg_check(sockaddr_in clientaddr){
    char ip[INET_ADDRSTRLEN];
    uint16_t port;
    inet_ntop(AF_INET, &(clientaddr.sin_addr), ip, sizeof (ip));
    port = htons(clientaddr.sin_port);
    string ip_str(ip);

    for(int i = 0; i < tablet_addresses.size(); i++){
        if(clientaddr.sin_addr.s_addr== tablet_addresses[i].sin_addr.s_addr && 
        clientaddr.sin_port == tablet_addresses[i].sin_port){
            return true;
        }
    }
    return false;
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

string get_time(){
    string time_str;
    struct timeval val;
    struct timezone zone;
    struct tm *time;
    gettimeofday(&val, &zone);
    time = localtime(&val.tv_sec);
    char buffer[25];
    sprintf(buffer, "%02d:%02d:%02d:%06ld", time->tm_hour, time->tm_min, time->tm_sec, val.tv_usec/1000);
    string server_idx_str = to_string(curr_server_index);
    time_str = string(buffer) = string(buffer) + " " + "S" + server_idx_str; 
    return time_str;
}


vector<int> find_my_replica_group(string request_str){
    PennCloud::Request request;
    request.ParseFromString(request_str);
    int start_letter = request.rowkey()[0];
    vector<int> my_tablet_server_group;
    for(int i = 0; i < rowkey_range.size(); i++){
        if(start_letter >= toRowKeyRange(rowkey_range[i]).first && start_letter <= toRowKeyRange(rowkey_range[i]).second){
            my_tablet_server_group = tablet_server_group[toKey(toRowKeyRange(rowkey_range[i]).first, toRowKeyRange(rowkey_range[i]).second)];
            break;
        }    
    }
    return my_tablet_server_group;
}

pair<int,int> find_rowkey_range(string request_str){
    PennCloud::Request request;
    request.ParseFromString(request_str);
    int start_letter = request.rowkey()[0];
    pair<int,int> rkey_range;
    for(int i = 0; i < rowkey_range.size(); i++){
        if(start_letter >= toRowKeyRange(rowkey_range[i]).first && start_letter <= toRowKeyRange(rowkey_range[i]).second){
            rkey_range.first = toRowKeyRange(rowkey_range[i]).first;
            rkey_range.second = toRowKeyRange(rowkey_range[i]).second;
            //break;
        }    
    }
    return rkey_range;
}

string update_kv_store(string request_str, int client_socket){
    PennCloud::Request request;
    request.ParseFromString(request_str);
    PennCloud::Response response;
    string response_str;
    if (strcasecmp(request.type().c_str(), "PUT") == 0){
		update_log(log_file_name,meta_log_file_name,request.type(),request.rowkey(),request.columnkey(),request.value1(),request.value2());
		process_put_request(request, response);
	}
	else if (strcasecmp(request.type().c_str(), "CPUT") == 0){
		update_log(log_file_name,meta_log_file_name,request.type(),request.rowkey(),request.columnkey(),request.value1(),request.value2());
		process_cput_request(request, response);
	}
	else if (strcasecmp(request.type().c_str(), "DELETE") == 0){
		update_log(log_file_name,meta_log_file_name,request.type(),request.rowkey(),request.columnkey(),request.value1(),request.value2());
		process_delete_request(request, response);
	}
    response.SerializeToString(&response_str);
    if(!isPrimary){
        //send an ACK
        cout<<"Sending an ACK to primary"<<endl;
        int sockfd = socket(PF_INET, SOCK_STREAM, 0);
        if (sockfd < 0) {
            if(verbose)
                cerr<<"Unable to create socket for Replication"<<endl;
                exit(-1);
        }
        pair<int,int> my_rkey_range = find_rowkey_range(request_str);
        int unique_key = toKey(my_rkey_range.first, my_rkey_range.second);
        if(connect(sockfd, (struct sockaddr*)& tablet_addresses[rkey_to_primary[unique_key]], 
        sizeof(tablet_addresses[rkey_to_primary[unique_key]]))<0)
        { 
            cerr<<"Connect Failed in update kv store: "<<errno<<endl;
            close(sockfd);
            return response_str;
        }
        request.set_command("ACK");
        request.set_isserver("true");
        string ack_request_str;
        request.SerializeToString(&ack_request_str);
        //ack_request_str += "\r\n";
        cout<<"Sending ACK"<<endl;
        //int return_val = write(sockfd, ack_request_str.c_str(), strlen(ack_request_str.c_str()));
        char req_length[11];
        snprintf (req_length, 11, "%10d", ack_request_str.length()); 
        std::string message = std::string(req_length) + ack_request_str;
        do_write(sockfd, message.data(), message.length());
        close(sockfd);
    }
    return response_str;
}

void update_secondary(string request_str){
    PennCloud::Request request;
    request.ParseFromString(request_str);
    request.set_command("WRITE");
    request.set_isserver("true");
    string write_request_str;
    request.SerializeToString(&write_request_str);
    cout<<"Write String: "<<write_request_str<<endl;
    pair<int,int> my_rkey_range = find_rowkey_range(request_str);
    vector<int> my_tablet_server_group = tablet_server_group[toKey(my_rkey_range.first, my_rkey_range.second)];
    for(int i = 0; i < my_tablet_server_group.size(); i++){
        if(my_tablet_server_group[i]!=curr_server_index){
            int sockfd = socket(PF_INET, SOCK_STREAM, 0);
            if (sockfd < 0) {
                if(verbose)
                    cerr<<"Unable to create socket for Replication"<<endl;
                return;
            }
            if(connect(sockfd, (struct sockaddr*)& tablet_addresses[my_tablet_server_group[i]], sizeof(tablet_addresses[my_tablet_server_group[i]]))<0)
            {
                cerr<<"Connect Failed in update secondary: "<<errno<<endl;
            close(sockfd);                
            continue;
            }
            //write_request_str +=  "\r\n";
            cout<<"Sending WRITE"<<endl;
            char req_length[11];
            snprintf (req_length, 11, "%10d", write_request_str.length()); 
            std::string message = std::string(req_length) + write_request_str;
            do_write(sockfd, message.data(), message.length());     
            close(sockfd);        
        } 
    }
}

//grant request to the owner secondary
void grant_secondary(string request_str){
    PennCloud::Request request;
    request.ParseFromString(request_str);
    request.set_command("GRANT");
    request.set_isserver("true");
    string write_request_str;
    request.SerializeToString(&write_request_str);

    pair<int,int> my_rkey_range = find_rowkey_range(request_str);
    vector<int> my_tablet_server_group = tablet_server_group[toKey(my_rkey_range.first, my_rkey_range.second)];
    for(int i = 0; i < my_tablet_server_group.size(); i++){
        if(to_string(my_tablet_server_group[i])==request.sender_server_index()){
            int sockfd = socket(PF_INET, SOCK_STREAM, 0);
            if (sockfd < 0) {
                if(verbose)
                    cerr<<"Unable to create socket for Replication"<<endl;
                return;
            }
            if(connect(sockfd, (struct sockaddr*)& tablet_addresses[my_tablet_server_group[i]], sizeof(tablet_addresses[my_tablet_server_group[i]]))<0)
            {    
                cerr<<"Connect Failed in grant secondary: "<<errno<<endl;
            close(sockfd);
            continue;
            }

            //write_request_str += "\r\n";
            cout<<"Sending GRANT"<<endl;
            char req_length[11];
            snprintf (req_length, 11, "%10d", write_request_str.length()); 
            std::string message = std::string(req_length) + write_request_str;
            do_write(sockfd, message.data(), message.length());     
            close(sockfd);            
            break;
        } 
    }
}

//send the request to primary
void request_primary(string request_str){
    //send a REQUEST to primary
    cout<<"Sending an REQUEST to primary"<<endl;
    PennCloud::Request request;
    request.ParseFromString(request_str);
    int sockfd = socket(PF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        if(verbose)
            cerr<<"Unable to create socket for Replication"<<endl;
            return;
    }
    pair<int,int> my_rkey_range = find_rowkey_range(request_str);
    int unique_key = toKey(my_rkey_range.first, my_rkey_range.second);
    cout<<rkey_to_primary[unique_key]<<endl;
    if(connect(sockfd, (struct sockaddr*)& tablet_addresses[rkey_to_primary[unique_key]], 
    sizeof(tablet_addresses[rkey_to_primary[unique_key]]))<0) 
    {
        cerr<<"Connect Failed in req primary: "<<errno<<endl;
        return;
    }
    request.set_command("REQUEST");
    request.set_isserver("true");   
    string req_request_str;
    request.SerializeToString(&req_request_str);
    //req_request_str += "\r\n";
    cout<<"Sending REQUEST"<<endl;
    char req_length[11];
    snprintf (req_length, 11, "%10d", req_request_str.length()); 
    std::string message = std::string(req_length) + req_request_str;
    do_write(sockfd, message.data(), message.length());     
    close(sockfd);      
    }