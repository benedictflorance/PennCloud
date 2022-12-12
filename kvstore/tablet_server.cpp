#include "utils/globalvars.h"
#include "utils/hash.h"
#include "utils/command_processor.h"
#include "utils/tools.h"
#include "utils/log.h"
#include "utils/config_processor.h"
#include "utils/update_manager.h"
#include "utils/background_daemons.h"
#include "utils/replication.h"
#include "utils/recovery.h"

void process_client_thread(int client_socket);
int create_server();
void process_request(string request_str, int client_socket);

char *sstrstr(char *haystack, char *needle, size_t length)
{
    size_t needle_length = strlen(needle);
    size_t i;
    for (i = 0; i < length; i++) {
        if (i + needle_length > length) {
            return NULL;
        }
        if (strncmp(&haystack[i], needle, needle_length) == 0) {
            return &haystack[i];
        }
    }
    return NULL;
}

// function for reading
bool do_read(int fd, char *buf, int len){
  int rcvd = 0;
  while (rcvd < len)
  {
    int n = read(fd, &buf[rcvd], len - rcvd);
    if (n < 0)
      return false;
    rcvd += n;
	
  }
  return true;
}

void create_log_file(){
	log_file_name = log_dir + "tablet_log_"+ to_string(curr_server_index)+".txt";
	meta_log_file_name = meta_log_dir + "tablet_log_"+ to_string(curr_server_index)+".txt";
	create_dir("checkpoints/");
	create_dir("metadata/");
	create_dir(log_dir);
	create_dir("checkpoints/");
	create_dir("metadata/");
	create_dir(meta_log_dir);
	create_file(log_file_name);
	create_file(meta_log_file_name);
}

void process_request(string request_str, int client_socket){
	PennCloud::Request request;
	request.ParseFromString(request_str);
	PennCloud::Response response;
	string response_str;
	if(!request.has_type()){
		response.set_status(type_unset_message.first);
		response.set_description(type_unset_message.second);
		response.SerializeToString(&response_str);
	}
	else if (strcasecmp(request.type().c_str(), "GET") == 0){
		process_get_request(request, response);
		response.SerializeToString(&response_str);
	}
	else if((strcasecmp(request.type().c_str(), "PUT") == 0) || 
					(strcasecmp(request.type().c_str(), "CPUT") == 0) ||
					(strcasecmp(request.type().c_str(), "DELETE") == 0))
	{
		response_str = request.preprocessed_response();
		response.ParseFromString(response_str);
	}
	else{
		response.set_status(unrecognized_command_message.first);
		response.set_description(unrecognized_command_message.second);
		response.SerializeToString(&response_str);
	}	
										
	//write(client_socket, response_str.c_str(), strlen(response_str.c_str()));
	char req_length[11];
	snprintf (req_length, 11, "%10d", response_str.length()); 
	std::string message = std::string(req_length) + response_str;
	do_write(client_socket, message.data(), message.length());
	if(verbose)
	{
		// [N] C: <text> (where <text> is a command received from the client and N is as above);
		// [N] S: <text> (where <text> is a response sent by the server, and N is as above);
		cerr<<"["<<client_socket<<"] "<<"Server sent a response status of  "<<response.status()<<" response description of "<<response.description()<<" response value of "<<response.value().length()<<endl;
	}
}

void process_client_thread(int client_socket)
{	
	if(verbose)
	{
		// [N] New connection (where N is the file descriptor of the connection);
		cerr<<"["<<client_socket<<"] "<<new_connection_message<<endl;
	}
	char length_buffer[LENGTH_BUFFER_SIZE];
	char *request_buffer = new char[BUFFER_SIZE];
	while(true)
	{	
		// char *command_end_index;
		// int client_shutdown = read(client_socket, current_buffer, BUFFER_SIZE-strlen(net_buffer));
		if(shutdown_flag)
		{
			if(verbose)
					cerr<<"["<<client_socket<<"] "<<closing_message<<endl;
			write(client_socket, shutdown_message, strlen(shutdown_message));
			pthread_exit(NULL);
		}
		memset(length_buffer, 0, sizeof(length_buffer));
		memset(request_buffer, 0, sizeof(request_buffer));
		do_read(client_socket, length_buffer, 10);
		int request_length;
		try
		{
			request_length = stoi(string(length_buffer, 10));
		}
		catch (const std::invalid_argument& e)
		{
			cout<<"stoi fail in buffering"<<endl;
			break;
		}
		do_read(client_socket, request_buffer, request_length);
		string request_str = string(request_buffer, request_length);

		PennCloud::Request request;
		request.ParseFromString(request_str);
		PennCloud::Response response;
		string response_str;
		if(verbose)
			cerr<<"["<<client_socket<<"] "<<"Client received a request type of "<<request.type()<<" a rowkey of "<<request.rowkey()
			<<" a columnkey of "<<request.columnkey()<<" a value1 of "<<request.value1().length()
			<<" a value2 of "<<request.value2().length()<<endl; 
		//message from another server
		if(request.isserver() == "true"){
			//this is the secondary server receiving a WRITE message for replication protocol
			if(request.command() == "WRITE"){

				int expected_seq_no;
				do{
				if(rowkey_version.find(request.rowkey()) == rowkey_version.end())
					expected_seq_no = 1;
				else
					expected_seq_no = rowkey_version[request.rowkey()] + 1;
					cout<<"Expected sequence number now is "<<expected_seq_no<<" and current request seq number is "<<request.sequence_number()<<endl;
				}while(expected_seq_no != stoi(request.sequence_number())); // expected is 3, and request now is 5 (5 just gets added to holdback and does not busy wait), when 3 comes in, it finished [OPTIMIZATION]
				//locally update and send an ACK
				update_kv_store(request_str);
				// Update the last seen sequence number
				rowkey_version_lock[request.rowkey()].lock();
				rowkey_version[request.rowkey()] = expected_seq_no;
				rowkey_version_lock[request.rowkey()].unlock();
			}
			//this is the primary server receiving request to WRITE
			else if(request.command() == "REQUEST"){
				//locally update
				rowkey_version_lock[request.rowkey()].lock();
				// Initialize seq to 0 if it sees rkey for first time, else increment
				if(rowkey_version.find(request.rowkey()) == rowkey_version.end())
				{
					rowkey_version[request.rowkey()] = 1;
				}
				else
				{
					rowkey_version[request.rowkey()]++;
				}
				rowkey_version_lock[request.rowkey()].unlock();
				request.set_sequence_number(to_string(rowkey_version[request.rowkey()]));
				string response_str = update_kv_store(request_str);
				request.set_preprocessed_response(response_str);
				string new_request_str;
				request.SerializeToString(&new_request_str);
				//ask secondary servers to update
				update_secondary(new_request_str);
				//add the msg to holdback queue - TODO
			}
			//this is the primary server, receiving an ACK message about a request
			else if(request.command() == "ACK"){
				cout<<"received ACK"<<endl;
				//update holdback queue - TODO
				//count ACKs per request_str
				//TODO: eliminate curr_server_index
				if(number_of_acks.find(request.uniqueid()) == number_of_acks.end()){
					number_of_acks[request.uniqueid()]=0;
				}
				number_of_acks[request.uniqueid()]++;
				cout<<"Number of ACKs received: " << number_of_acks[request.uniqueid()]<<endl;
				//if message received from all secondaries
				int num_of_secondaries, start_letter = request.rowkey()[0];
				int key;
				for(int i = 0; i < rowkey_range.size(); i++)
				{
					if(start_letter >= toRowKeyRange(rowkey_range[i]).first && start_letter <= toRowKeyRange(rowkey_range[i]).second)
					{
						key = rowkey_range[i];
					}
				}
				if(number_of_acks[request.uniqueid()] == tablet_server_group[key].size() - 1){
					// own message, process
					cout<<"Process Message"<<endl;
					if(request.sender_server_index() == to_string(curr_server_index)){
						process_request(request_str, req_client_sock_map[request.uniqueid()]);
					}
					//else GRANT
					else{
						grant_secondary(request_str);
					}
				}
			}
			//this is a secondary server, request was granted by the primary and can now be processed
			else if(request.command() == "GRANT"){
				cout<<"received GRANT"<<endl;
				//process request
				process_request(request_str, req_client_sock_map[request.uniqueid()]);
			}
			//this is a secondary server, CHECKPOINT command is sent by the primary
			else if ((strcasecmp(request.type().c_str(), "CHECKPOINT") == 0)){
				thread sec_checkpoint_thread(checkpoint_kvstore_secondary);
				sec_checkpoint_thread.detach();
			}
			else if ((strcasecmp(request.type().c_str(), "NEW_PRIMARY") == 0)){
				// is new_primary
				if(curr_server_index == stoi(request.new_primary_index())){
					cout<<"I am the new primary "<<to_string(curr_server_index)<<endl;
					isPrimary = true;
				}
				// is not the new primary
				else{
					cout<<"I am not the primary: "<<to_string(curr_server_index)<<endl;
					isPrimary = false;
				}
				for(int i = 0; i < rowkey_range.size(); i++){
					rkey_to_primary[rowkey_range[i]] = stoi(request.new_primary_index());
					// for(int j = 0; j < tablet_server_group[rowkey_range[i]].size(); j++)
					// {
						cout<<"Before: Size of my group is"<<tablet_server_group[rowkey_range[i]].size()<<endl;
						auto it = find(tablet_server_group[rowkey_range[i]].begin(),
						tablet_server_group[rowkey_range[i]].end(),
						stoi(request.modified_server_index()));
						tablet_server_group[rowkey_range[i]].erase(it);
						cout<<"After: Size of my group is"<<tablet_server_group[rowkey_range[i]].size()<<endl;
					// }
				}
			}
			else if ((strcasecmp(request.type().c_str(), "RESURRECT") == 0)){
				int resurrected_server_index = stoi(request.modified_server_index());
				for(auto it : initial_tablet_server_group)
				{
					if(find(it.second.begin(), it.second.end(), resurrected_server_index) != it.second.end())
					{
						tablet_server_group[it.first].push_back(resurrected_server_index);
					}
				}
				if(resurrected_server_index == curr_server_index){
					ask_sequence_numbers();
					get_checkpoint_from_primary();
				}
			}
			else if ((strcasecmp(request.type().c_str(), "SEQUENCE") == 0)){
				int requesting_server_index = stoi(request.sender_server_index());
				send_sequence_numbers(requesting_server_index);
			}
			else if ((strcasecmp(request.type().c_str(), "SEQUENCEREPLY") == 0)){
				rowkey_version.clear();
				for(auto item : (*request.mutable_rowkey_version()))
				{
					rowkey_version[item.first] = item.second;
				}
			}		
		}
		//request from client
		else{
			cout<<"Request from Client"<<endl;
			if((strcasecmp(request.type().c_str(), "PUT") == 0) || 
				(strcasecmp(request.type().c_str(), "CPUT") == 0) ||
				(strcasecmp(request.type().c_str(), "DELETE") == 0))
			{
				request.set_uniqueid(get_time() + to_string(rand()));
				req_client_sock_map[request.uniqueid()] = client_socket;
				if(is_rowkey_accepted(request.rowkey()))
				{	
					request.set_sender_server_index(to_string(curr_server_index));
					//TO DO - make it more unique
					string new_request_str;
					
					if(isPrimary){
						rowkey_version_lock[request.rowkey()].lock();
						// Initialize seq to 0 if it sees rkey for first time, else increment
						if(rowkey_version.find(request.rowkey()) == rowkey_version.end())
						{
							rowkey_version[request.rowkey()] = 1;
						}
						else
						{
							rowkey_version[request.rowkey()]++;
						}
						rowkey_version_lock[request.rowkey()].unlock();
						request.set_sequence_number(to_string(rowkey_version[request.rowkey()]));
						//locally update
						request.SerializeToString(&new_request_str);
						string preprocessed_response = update_kv_store(new_request_str);
						request.set_preprocessed_response(preprocessed_response);
						request.SerializeToString(&new_request_str);
						int start_letter = request.rowkey()[0];
						int key;
						for(int i = 0; i < rowkey_range.size(); i++)
						{
							if(start_letter >= toRowKeyRange(rowkey_range[i]).first && start_letter <= toRowKeyRange(rowkey_range[i]).second)
							{
								key = rowkey_range[i];
							}
						}
						//ask secondary servers to update
						if(tablet_server_group[key].size()!= 1)
						{
							update_secondary(new_request_str);
						}
						else
						{
							process_request(new_request_str, req_client_sock_map[request.uniqueid()]);
						}
					}
					else{
						//request primary for permission
						// unique id to value1() - hash it here
						reqid_to_value.insert({request.uniqueid(), make_pair(request.value1(), request.value2())});
						request.SerializeToString(&new_request_str);
						request_primary(new_request_str);
					}
				}
				else
				{
					PennCloud::Response response;
					response.set_status(invalid_rowkey_message.first);
					response.set_description(invalid_rowkey_message.second);
					string preprocessed_response;
					response.SerializeToString(&preprocessed_response);
					request.set_preprocessed_response(preprocessed_response);
					string new_request_str;
					request.SerializeToString(&new_request_str);
					process_request(new_request_str, req_client_sock_map[request.uniqueid()]);
				}
			}
			else
			{
				process_request(request_str, client_socket);
			}
		}
	}
	delete request_buffer;
}

int create_server()
{
	/*
	If the user terminates the server by pressing Ctrl+C, the server should write -ERR Server shutting down to each open connection and then close all open
	sockets before terminating.   
	*/
	signal(SIGINT, signal_handler);
	// Your server should open a TCP port and start accepting connections. 
	// Create a socket file descriptor
	if((server_socket = socket(PF_INET, SOCK_STREAM, 0)) < 0)
	{
			cerr << "Socket creation error!\r\n";
			return -1;
	}
	if (::bind(server_socket, (struct sockaddr*)& tablet_addresses[curr_server_index], sizeof(tablet_addresses[curr_server_index])) < 0) 	
	{
	cerr << "Binding error!\r\n";
	return -1;
	}
	if (listen(server_socket, 100) < 0) {
		cerr << "Listen error!\r\n";
		return -1;
	}

	//create a thread for sending heartbeats to the master
	thread send_heartbeats(send_heartbeat);
	send_heartbeats.detach();
	if(isPrimary)
	{
		thread checkpoint_thread(checkpoint_kvstore_primary);
		checkpoint_thread.detach();
	}

	while(true)
	{
		struct sockaddr_in clientaddr;
		socklen_t clientaddrlen = sizeof(clientaddr);
		int client_socket;
		if((client_socket = accept(server_socket, (struct sockaddr*)&clientaddr, &clientaddrlen)) < 0)
		{
			cerr << "Acceptance error!\r\n";
			return -1;			
		}
		//check if msg is from a fellow tablet server
		client_sockets_mutex.lock();
		client_sockets.push_back(client_socket);
		if(verbose)
			cerr<<("Connection from %s\n", inet_ntoa(clientaddr.sin_addr));
		thread client_thread(process_client_thread, client_sockets.back());
		client_threads.push_back(client_thread.native_handle());	
		client_thread.detach();
		client_sockets_mutex.unlock();
	}
	return 0;
}


int main(int argc, char *argv[])
{
	GOOGLE_PROTOBUF_VERIFY_VERSION;
    int option;
    while((option = getopt(argc, argv, "v")) != -1)
	{   
		switch(option)
		{
			case 'v':
					verbose = true;
					break;
			default:
					// Invalid option
					cerr<<"Option not recognized!\r\n";
					return -1;
		}
	}
    if(optind == argc)
    {
        cerr<<"Arguments missing. Please input [Configuration File] [Index of current server instance]"<<endl;
        exit(-1);
    }
    config_file = argv[optind];
    optind++;
    if(optind == argc)
    {
        cerr<<"Argument missing. Please input [Index of current server instance]"<<endl;
        exit(-1);
    }
    try
    {
        curr_server_index = stoi(argv[optind]); // 0-indexed
    }
    catch(const std::invalid_argument&)
    {
        cerr <<"Invalid server index."<<endl;
        exit(-1);
    }
    process_config_file(config_file);
	initialize_primary_info(config_file);
	//create log file if it doesn't exist
	create_log_file();
	load_kvstore_from_disk();
	replay_log(log_file_name, meta_log_file_name);
    int isSuccess = create_server();
	return isSuccess;
}