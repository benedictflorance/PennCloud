void process_config_file(string config_file);
sockaddr_in get_address(string socket_address);
void signal_handler(int arg);

sockaddr_in get_address(string socket_address)
{
    int colon_index = socket_address.find(":");
    string ip_address = socket_address.substr(0, colon_index);
    string port_str = socket_address.substr(colon_index + 1, socket_address.length() - colon_index - 1);
    if(ip_address.empty())
    {
            std::cout << ip_address << " " << port_str << std::endl;
        cerr<<invalid_ip_message;
        exit(-1);
    }
    int port;
    try
    {
        port = stoi(port_str);
    }
    catch(const std::invalid_argument&)
    {
        cerr <<invalid_ip_message;
        exit(-1);
    }
    struct sockaddr_in servaddr;
    bzero(&servaddr, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(port);
    inet_pton(AF_INET, ip_address.c_str(), &(servaddr.sin_addr));
    return servaddr;
}

void process_config_file(string config_file)
{
    ifstream config_fstream(config_file);
    string line;
    getline(config_fstream, line); // Get <MASTER>
    getline(config_fstream, line);
    std::size_t pos;
    pos = line.find("\r"); if (pos != std::string::npos) line.replace(pos, 1, "");
    master_address = get_address(line);
    getline(config_fstream, line); // Get <TABLETS>
    int index = 0;
    while(getline(config_fstream, line))
    {
        pos = line.find("\r");
        if (pos != std::string::npos) line.replace(pos, 1, "");
        if(line == replicas_header || line == (replicas_header + "\r"))
            break;
        if(index == curr_server_index && verbose)
            cerr<<"Listening on "<<line<<endl;
        tablet_addresses.push_back(get_address(line));
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
        if(find(server_indices.begin(), server_indices.end(), curr_server_index) != server_indices.end())
        {
            int start, end;
            int hyphen_index = row_range.find("-");
            start = row_range[0];
            end  = row_range[2];
            rowkey_range.push_back(toKey(start, end));
        }
    }
    if(verbose)
    {
        cerr<<"This server accepts requests for rowkey ranges: ";
        for(auto range : rowkey_range)
        {
            cerr<<(char) toRowKeyRange(range).first<<"-"<<(char) toRowKeyRange(range).second<<" ";
        }
        cerr<<endl;
    }
    curr_ip_addr = string(inet_ntoa(tablet_addresses[curr_server_index].sin_addr)) + ":" + to_string(ntohs(tablet_addresses[curr_server_index].sin_port));
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
        if(line == replicas_header)
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
        if(server_indices[0] == curr_server_index)
            isPrimary = true;
        if(verbose)
        {
            cout<<"Rowkey range "<<(char) start<<" to "<<(char) end<<" has the key as "<<toKey(start, end)
                <<" and primary server "<<rkey_to_primary[toKey(start, end)]<<" and tablet servers :";
            for(int i = 0; i < tablet_server_group[toKey(start, end)].size(); i++)
                cout<<tablet_server_group[toKey(start, end)][i]<<" ";
            cout<<endl;
        }
    }
}

void signal_handler(int arg)
{
	/*
	If the user terminates the server by pressing Ctrl+C, the server should write -ERR Server shutting down to each open connection and then close all open
	sockets before terminating.   
	*/
	shutdown_flag = true;
	close(server_socket);
    close(socket_to_master);
	cerr<<endl<<shutdown_message;
	for(int i = 0; i < client_sockets.size(); i++)
	{
		int status = fcntl(client_sockets[i], F_SETFL, fcntl(client_sockets[i], F_GETFL, 0) | O_NONBLOCK);
		if(status != -1) // Status is -1 implies, socket is already closed and thread is closed!
		{
			close(client_sockets[i]);
			pthread_kill(client_threads[i].native_handle(), 0);
		}
	}
	exit(-1);
}
void load_kvstore_from_disk()
{
    string latest_checkpt = "";
    int max_version = -1;
    for (const auto & entry : fs::directory_iterator(checkpt_dir))
    {
        string filename = entry.path().generic_string();
        if(filename.find(curr_ip_addr) != string::npos)
        {
            string version_str = filename.substr(filename.find_last_of('_') + 1);
            int file_version = stoi(version_str);
            if(latest_checkpt == "" || file_version > max_version)
            {
                latest_checkpt = filename;
                max_version = file_version;
            }
        }
    }
    if(latest_checkpt != "")
    {
        string check_filename = latest_checkpt;
        string meta_filename = latest_checkpt.replace(latest_checkpt.find(checkpt_dir), sizeof("checkpoints/") - 1, checkpt_meta_dir)
                                .replace(latest_checkpt.find("_checkpoint_"), sizeof("_checkpoint_") - 1, "_metadata_");
        if(verbose)
        {
            cout<<"Loading checkpoint from "<<check_filename<<endl;
            cout<<"Loading metadata from "<<meta_filename<<endl;
        }
        fstream checkpt_file(check_filename, ios::in);
        fstream meta_file(meta_filename, ios::in);
        string line;
        char *rkey_char = new char[RKEY_BUFFER_SIZE];
        char *ckey_char = new char[CKEY_BUFFER_SIZE];
        char *value_char = new char[BUFFER_SIZE];
        int count = 0;
        while(getline(meta_file, line))
        {
            count++;
            stringstream meta(line); 
            string rkey_start_str, rkey_size_str, ckey_start_str, ckey_size_str, val_start_str, val_size_str;
            getline(meta, rkey_start_str, ',');
            getline(meta, rkey_size_str, ',');
            getline(meta, ckey_start_str, ',');
            getline(meta, ckey_size_str, ',');
            getline(meta, val_start_str, ',');
            getline(meta, val_size_str, ',');
            int rkey_start = stoi(rkey_start_str), 
                rkey_size = stoi(rkey_size_str), 
                ckey_start = stoi(ckey_start_str), 
                ckey_size = stoi(ckey_size_str), 
                val_start = stoi(val_start_str), 
                val_size= stoi(val_size_str);
            memset(rkey_char, 0, sizeof(rkey_char));
            memset(ckey_char, 0, sizeof(ckey_char));
            memset(value_char, 0, sizeof(value_char));
            checkpt_file.seekg(rkey_start, ios::beg);
            checkpt_file.read(rkey_char, rkey_size);
            checkpt_file.seekg(ckey_start, ios::beg);
            checkpt_file.read(ckey_char, ckey_size);
            checkpt_file.seekg(val_start, ios::beg);
            checkpt_file.read(value_char, val_size);
            string rkey = string(rkey_char), ckey = string(ckey_char), value = string(value_char);
            if(kv_store.find(rkey) != kv_store.end())
            {
                kv_store[rkey][ckey] = value;              
            }
            else
            {
                kv_store[rkey] = unordered_map<string, string>();
                kv_store[rkey][ckey] = value;   
            }
        }
        delete rkey_char;
        delete ckey_char;
        delete value_char;
        if(verbose)
            cerr<<"Checkpoint loaded into memory with "<<count<<" k-v pairs"<<endl;
    }
}