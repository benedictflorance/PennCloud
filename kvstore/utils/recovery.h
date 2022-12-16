void get_checkpoint_from_primary();
void restore_sequence_numbers();
void get_checkpoint_from_primary()
{
    // Get any rowkey range of this server
    int rkey_range = rowkey_range[0];
    // Find primary
    int primary_index = rkey_to_primary[rkey_range];
    string ip_port_str = string(inet_ntoa(tablet_addresses[primary_index].sin_addr)) 
                    + ":" + to_string(ntohs(tablet_addresses[primary_index].sin_port));
    load_kvstore_from_disk(ip_port_str);
    string log_filename = log_dir + "tablet_log_" + to_string(primary_index) + ".txt";
    string meta_filename = meta_log_dir + "tablet_log_" + to_string(primary_index) + ".txt";
    replay_log(log_filename, meta_filename);
}
void ask_sequence_numbers()
{
    // Establish connection with primary
    // Get any rowkey range of this server
    int rkey_range = rowkey_range[0];
    // Find primary
    int primary_index = rkey_to_primary[rkey_range];
    int socket_to_primary = socket(PF_INET, SOCK_STREAM, 0);
    if (socket_to_primary < 0) 
    {
        if(verbose)
            cerr<<"Unable to create socket"<<endl;
    }
    if(connect(socket_to_primary, (struct sockaddr*)& tablet_addresses[primary_index], 
            sizeof(tablet_addresses[primary_index])) < 0)
    {
        cerr<<"Connect failed while contacting server for new primary"<<endl;
    }
    string request_str;
    PennCloud::Request request;
    request.set_type("RECOVERY");
    request.set_isserver("true");
    request.set_sender_server_index(to_string(curr_server_index));
    request.SerializeToString(&request_str);
    char req_length[11];
    snprintf (req_length, 11, "%10d", request_str.length()); 
    std::string message = std::string(req_length) + request_str;
    do_write(socket_to_primary, message.data(), message.length());
    close(socket_to_primary);
}

void send_sequence_numbers(int requesting_server_index)
{
    int socket_to_server = socket(PF_INET, SOCK_STREAM, 0);
    if (socket_to_server < 0) 
    {
        if(verbose)
            cerr<<"Unable to create socket"<<endl;
    }
    if(connect(socket_to_server, (struct sockaddr*)& tablet_addresses[requesting_server_index], 
            sizeof(tablet_addresses[requesting_server_index])) < 0)
    {
        cerr<<"Connect failed while contacting server for new primary"<<endl;
    }
    string request_str;
    PennCloud::Request request;
    request.set_type("RECOVERYREPLY");
    request.set_isserver("true");
    for(auto item : rowkey_version)
    {
        (*request.mutable_rowkey_version())[item.first] = item.second;
    }
    request.SerializeToString(&request_str);
    char req_length[11];
    snprintf (req_length, 11, "%10d", request_str.length()); 
    std::string message = std::string(req_length) + request_str;
    do_write(socket_to_server, message.data(), message.length());
    close(socket_to_server);
}
