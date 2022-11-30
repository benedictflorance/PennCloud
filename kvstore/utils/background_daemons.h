//placeholder for sending heartbeats
void send_heartbeat();
void checkpoint_kvstore();
void checkpoint_kvstore_primary();
void checkpoint_kvstore_secondary();
void send_heartbeat(){
	//connect with master
	socket_to_master = socket(PF_INET, SOCK_STREAM, 0);
    if (socket_to_master < 0) 
    {
        if(verbose)
            cerr<<"Unable to create socket"<<endl;
    }
    connect(socket_to_master, (struct sockaddr*)& master_address, sizeof(master_address));

	//use update manager library
	while(true){
		auto t = UpdateManager::start();
		this_thread::sleep_for(2s);

		//send ALIVE command at fixed intervals
		// if(verbose)
		// 	cout<<"Sending Alive message to the master"<<endl;
		if(shutdown_flag)
		{
			close(socket_to_master);
			pthread_exit(NULL);
		}
        char str[INET_ADDRSTRLEN];
        inet_ntop(PF_INET, &tablet_addresses[curr_server_index], str, INET_ADDRSTRLEN);
        string ip(str);
        string alive = "ALIVE-" + to_string(curr_server_index)+ "\r\n";
		write(socket_to_master, alive.c_str(), strlen(alive.c_str()));
	}
}
void ask_secondaries_to_checkpoint()
{
    string request_str;
    PennCloud::Request request;
    request.set_type("CHECKPOINT");
    request.set_isserver("true");
    request.set_sender_server_index(to_string(curr_server_index));
    request.SerializeToString(&request_str);
    request_str += "\r\n";
    for(int i = 0; i < rowkey_range.size(); i++)
    {
        for(int j = 1; j < tablet_server_group[rowkey_range[i]].size(); j++)
        {
            int socket_to_secondary = socket(PF_INET, SOCK_STREAM, 0);
            if (socket_to_secondary < 0) 
            {
                if(verbose)
                    cerr<<"Unable to create socket"<<endl;
            }
            if(connect(socket_to_secondary, (struct sockaddr*)& tablet_addresses[tablet_server_group[rowkey_range[i]][j]], 
                    sizeof(tablet_addresses[tablet_server_group[rowkey_range[i]][j]])) < 0)
            {
                cerr<<"Primary server "<<curr_server_index<<" is trying to start checkpointing for secondary server "<<tablet_server_group[rowkey_range[i]][j]<<" but the secondary server is down :("<<endl;
                continue;
            }
            write(socket_to_secondary, request_str.c_str(), strlen(request_str.c_str()));
            cerr<<"Primary server "<<curr_server_index<<" asked secondary server "<<tablet_server_group[rowkey_range[i]][j]<<" to checkpoint"<<endl;
            close(socket_to_secondary);
        }
    }
}
void checkpoint_kvstore_primary()
{
	while(true){
		auto t = UpdateManager::start();
		this_thread::sleep_for(60s);
        // Before starting to checkpoint, send start checkpoint command to other secondaries
        ask_secondaries_to_checkpoint();
        checkpoint_kvstore_secondary();
	}

}
void checkpoint_kvstore_secondary()
{
    time_t timeInSec;
    time(&timeInSec);
    string time_string = to_string((long long) timeInSec);
    if(verbose)
        cout<<"Checkpoint started at "<<time_string<<endl;
    // Find next version number
    int max_version = -1;
    for (const auto & entry : fs::directory_iterator(checkpt_dir))
    {
        string filename = entry.path().generic_string();
        if(filename.find(curr_ip_addr) != string::npos)
        {
            string version_str = filename.substr(filename.find_last_of('_') + 1);
            int file_version = stoi(version_str);
            if(file_version > max_version)
            {
                max_version = file_version;
            }
        }
    }
    fstream checkpt_file(checkpt_dir + curr_ip_addr + "_checkpoint_" + to_string(max_version + 1), ios::out);
    fstream meta_file(checkpt_meta_dir + curr_ip_addr + "_metadata_" + to_string(max_version + 1), ios::out);
    for(auto &rowlock : rowkey_lock)
        rowlock.second.lock();
    // Start checkpointing
    if(kv_store.size() > 0)
    {
        for(auto &rowkey : kv_store)
        {
            for(auto &colkey : rowkey.second)
            {
                string rkey = rowkey.first, ckey = colkey.first, value = colkey.second;
                int rkey_start = checkpt_file.tellg();
                checkpt_file<<rkey; // Dump it as string itself
                int ckey_start = checkpt_file.tellg();
                checkpt_file<<ckey; // Dump it as string itself                   
                int val_start = checkpt_file.tellg();
                checkpt_file<<value;  // Value is assumed to be in bytes already, so we dump whaterver we get
                meta_file<<rkey_start<<","<<rkey.size()<<","<<ckey_start<<","<<ckey.size()<<","<<val_start<<","<<value.size()<<endl;                  
            }
        }
    checkpt_file.close();
    meta_file.close();
    for(auto &rowlock : rowkey_lock)
        rowlock.second.unlock();    
    time(&timeInSec);
    time_string = to_string((long long) timeInSec);
    if(verbose)
        cout<<"Checkpoint complete at "<<time_string<<endl;
    }
    clear_log(log_file_name, meta_log_file_name);
}