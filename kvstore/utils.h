void process_config_file();
sockaddr_in get_address(string socket_address);
sockaddr_in get_address(string socket_address)
{
    int colon_index = socket_address.find(":");
    string ip_address = socket_address.substr(0, colon_index);
    string port_str = socket_address.substr(colon_index + 1, socket_address.length() - colon_index - 1);
    if(ip_address.empty())
    {
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
    master_address = get_address(line);
    getline(config_fstream, line); // Get <TABLETS>
    int index = 0;
    while(getline(config_fstream, line))
    {
        if(line == "<REPLICAS>")
            break;
        if(index == curr_server_index && verbose)
            cerr<<"Listening on "<<line<<endl;
        tablet_addresses.push_back(get_address(line));
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
        if(find(server_indices.begin(), server_indices.end(), curr_server_index) != server_indices.end())
        {
            int start, end;
            int hyphen_index = row_range.find("-");
            start = row_range[0];
            end  = row_range[2];
            rowkey_range.push_back(make_pair(start, end));
        }
    }
    if(verbose)
    {
        cerr<<"This server accepts requests for rowkey ranges: ";
        for(auto pair : rowkey_range)
        {
            cerr<<(char) pair.first<<"-"<<(char) pair.second<<" ";
        }
        cerr<<endl;
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
	cerr<<endl<<shutdown_message;
	for(int i = 0; i < client_sockets.size(); i++)
	{
		int status = fcntl(client_sockets[i], F_SETFL, fcntl(client_sockets[i], F_GETFL, 0) | O_NONBLOCK);
		if(status != -1) // Status is -1 implies, socket is already closed and thread is closed!
		{
			write(client_sockets[i], shutdown_message, strlen(shutdown_message));
			close(client_sockets[i]);
			pthread_kill(client_threads[i], 0);
		}
	}
	exit(-1);
}