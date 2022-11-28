class ReplicationMessage{
    public:
    string request_str;
    int server_index;

    std::ostream& MyClass::serialize(std::ostream &out);
    std::istream& MyClass::deserialize(std::istream &in);
};

std::ostream& ReplicationMessage::serialize(std::ostream &out) const {
    out<< request_str.length();
    out << ',' // seperator
    out << request_str ;
    out << ',' // seperator
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

bool replica_msg_check(struct sockaddr_in clientaddr){
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


}

void request_primary(string request_str){

}