//placeholder for sending heartbeats
void send_heartbeat();
void checkpoint_kvstore();
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
		if(verbose)
			cout<<"Sending Alive message to the master"<<endl;
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
void checkpoint_kvstore()
{
	while(true){
		auto t = UpdateManager::start();
		this_thread::sleep_for(60s);
        time_t timeInSec;
        time(&timeInSec);
		string time_string = to_string((long long) timeInSec);
        fstream checkpt_file(checkpt_dir + curr_ip_addr + "_checkpoint_" + time_string, ios::out);
        fstream meta_file(checkpt_meta_dir + curr_ip_addr + "_metadata_" + time_string, ios::out);
		if(verbose)
			cout<<"Checkpoint started at "<<time_string<<endl;
        if(kv_store.size() > 0)
        {
            for(auto &rowkey : kv_store)
            {
                for(auto &colkey : rowkey.second)
                {
                    string rkey = rowkey.first, ckey = colkey.first, value = colkey.second;
                    cout<<rkey<<" "<<ckey<<" "<<value<<endl;
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
        time(&timeInSec);
		time_string = to_string((long long) timeInSec);
        //clear log files
        //clear_log(log_file_name, meta_log_file_name)
		if(verbose)
			cout<<"Checkpoint complete at "<<time_string<<endl;
        }
	}

}