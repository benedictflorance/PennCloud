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
    cout<<clientaddr.sin_port<<endl;
    cout<<ip<<port<<endl;

    for(int i = 0; i < tablet_addresses.size(); i++){
        if(clientaddr.sin_addr.s_addr== tablet_addresses[i].sin_addr.s_addr && 
        clientaddr.sin_port == tablet_addresses[i].sin_port){
            return true;
            cout<<"we are here"<<endl;
        }
    }
    return false;
}

void update_kv_store(string request_str){
    PennCloud::Request request;
    request.ParseFromString(request_str);
    if(kv_store.find(request.rowkey()) != kv_store.end()){
        kv_store[request.rowkey()][request.columnkey()] = request.value1();              
    }
    else{
        kv_store[request.rowkey()] = unordered_map<string, string>();
        kv_store[request.rowkey()][request.columnkey()] = request.value1();
    }
}

void update_secondary(string request_str){
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

    for(int i = 0; i < my_tablet_server_group.size(); i++){
        if(my_tablet_server_group[i]!=curr_server_index){
            int sockfd = socket(PF_INET, SOCK_STREAM, 0);
            if (sockfd < 0) {
                if(verbose)
                    cerr<<"Unable to create socket for Replication"<<endl;
                return;
            }
            //send using UDP??
            connect(sockfd, (struct sockaddr*)& tablet_addresses[my_tablet_server_group[i]], sizeof(tablet_addresses[my_tablet_server_group[i]]));
            
            request_str += "\r\n";
            write(sockfd, request_str.c_str(), strlen(request_str.c_str()));
        } 
    }
    cout<<"DONE"<<endl;

}

void request_primary(string request_str){

}