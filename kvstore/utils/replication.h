class ReplicationMessage{
    public:
    string request_str;
    int server_index;

    std::ostream& serialize(std::ostream &out);
    std::istream& deserialize(std::istream &in);
};

std::ostream& ReplicationMessage::serialize(std::ostream &out) {
    out<< request_str.length();
    out << ',';// seperator
    out << request_str ;
    out << ',';// seperator
    out << server_index ;
    return out;
}
std::istream& ReplicationMessage::deserialize(std::istream &in) {
    if (in) {
        int len=0;
        char comma;
        in >> len;
        in >> comma; //read in the seperator
        if (in && len) {
            std::vector<char> tmp(len);
            in.read(tmp.data() , len); //deserialize characters of string
            request_str.assign(tmp.data(), len);
        }
        in >> comma;
        in >> server_index;
    }
    return in;
}

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

vector<int> find_my_replica_group(string request_str){
    PennCloud::Request request;
    request.ParseFromString(request_str);
    int start_letter = request.rowkey()[0];
    vector<int> my_tablet_server_group;
    for(int i = 0; i < rowkey_range.size(); i++){
        if(start_letter >= rowkey_range[i].first && start_letter <= rowkey_range[i].second){
            my_tablet_server_group = tablet_server_group[toKey(rowkey_range[i].first, rowkey_range[i].second)];
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
        if(start_letter >= rowkey_range[i].first && start_letter <= rowkey_range[i].second){
            rkey_range.first = rowkey_range[i].first;
            rkey_range.second = rowkey_range[i].second;
            //break;
        }    
    }
    return rkey_range;
}

void update_kv_store(string request_str, int client_socket = -1){
    PennCloud::Request request;
    request.ParseFromString(request_str);
    if(kv_store.find(request.rowkey()) != kv_store.end()){
        kv_store[request.rowkey()][request.columnkey()] = request.value1();              
    }
    else{
        kv_store[request.rowkey()] = unordered_map<string, string>();
        kv_store[request.rowkey()][request.columnkey()] = request.value1();
    }
    if(!isPrimary){
        //send an ACK
        cout<<"Sending an ACK to primary"<<endl;
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
            cerr<<"Connect Failed: "<<errno<<endl;

        request.set_command("ACK");
        string ack_request_str;
        request.SerializeToString(&ack_request_str);
        ack_request_str += ack_request_str + "\r\n";
        cout<<"Sending ACK"<<endl;
        write(sockfd, ack_request_str.c_str(), strlen(ack_request_str.c_str()));
        //close(sockfd);
    }
}

void update_secondary(string request_str){
    PennCloud::Request request;
    request.ParseFromString(request_str);
    request.set_command("WRITE");
    string write_request_str;
    request.SerializeToString(&write_request_str);

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
            connect(sockfd, (struct sockaddr*)& tablet_addresses[my_tablet_server_group[i]], sizeof(tablet_addresses[my_tablet_server_group[i]]));

            write_request_str += write_request_str + "\r\n";
            cout<<"Sending WRITE"<<endl;
            write(sockfd, write_request_str.c_str(), strlen(write_request_str.c_str()));
            //close(sockfd);
        } 
    }
}

//grant request to the owner secondary
void grant_secondary(string request_str){
    PennCloud::Request request;
    request.ParseFromString(request_str);
    request.set_command("GRANT");
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
            connect(sockfd, (struct sockaddr*)& tablet_addresses[my_tablet_server_group[i]], sizeof(tablet_addresses[my_tablet_server_group[i]]));

            write_request_str += write_request_str + "\r\n";
            cout<<"Sending GRANT"<<endl;
            write(sockfd, write_request_str.c_str(), strlen(write_request_str.c_str()));
            //close(sockfd);
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
        cerr<<"Connect Failed: "<<errno<<endl;
    
    request.set_command("REQUEST");
    string req_request_str;
    request.SerializeToString(&req_request_str);
    req_request_str += req_request_str + "\r\n";
    cout<<"Sending REQUEST"<<endl;
    write(sockfd, req_request_str.c_str(), strlen(req_request_str.c_str()));
    //close(sockfd);
}