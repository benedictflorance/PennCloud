#include "utils/command_processor.h"
#include "utils/tools.h"
#include "utils/log.h"
#include "utils/hash.h"
#include "utils/config_processor.h"
#include "utils/update_manager.h"
#include "utils/background_daemons.h"
#include "utils/replication.h"

void *process_client_thread(void *arg);
int create_server();

void create_log_file(){
	log_file_name = log_dir + "tablet_log_"+ to_string(curr_server_index)+".txt";
	meta_log_file_name = meta_log_dir + "tablet_log_"+ to_string(curr_server_index)+".txt";
	create_dir(log_dir);
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
	}
	else{
		response.set_status(unrecognized_command_message.first);
		response.set_description(unrecognized_command_message.second);
		response.SerializeToString(&response_str);
	}										
	write(client_socket, response_str.c_str(), strlen(response_str.c_str()));
	if(verbose)
	{
		// [N] C: <text> (where <text> is a command received from the client and N is as above);
		cerr<<"["<<client_socket<<"] "<<"Client received a request type of "<<request.type()<<" a rowkey of "<<request.rowkey()<<" a columnkey of "<<request.columnkey()<<" a value1 of "<<request.value1()
		<<" a value2 of "<<request.value2()<<endl; 
		// [N] S: <text> (where <text> is a response sent by the server, and N is as above);
		cerr<<"["<<client_socket<<"] "<<"Server sent a response status of  "<<response.status()<<" response description of "<<response.description()<<" response value of "<<response.value()<<endl;
	}
}

void *process_client_thread(void *arg)
{	
	int client_socket = *(int*) arg;
	if(verbose)
	{
		// [N] New connection (where N is the file descriptor of the connection);
		cerr<<"["<<client_socket<<"] "<<new_connection_message<<endl;
	}

	char net_buffer[BUFFER_SIZE];
	memset(net_buffer, 0, sizeof(net_buffer));
	char* current_buffer = net_buffer;

	while(true)
	{	
		char *command_end_index;
		int client_shutdown = read(client_socket, current_buffer, BUFFER_SIZE-strlen(net_buffer));
		if(shutdown_flag || client_shutdown == 0)
		{
			if(verbose)
					cerr<<"["<<client_socket<<"] "<<closing_message<<endl;
			write(client_socket, shutdown_message, strlen(shutdown_message));
			close(client_socket);
			pthread_exit(NULL);
		}
		while((command_end_index = strstr(net_buffer, "\r\n")) != NULL)
		{
			int full_command_length = command_end_index + suffix_length - net_buffer;
			string request_str = string(net_buffer, full_command_length);

			PennCloud::Request request;
			request.ParseFromString(request_str);

			//message from another server
			if(request.isserver() == "true"){
				//this is the secondary server receiving a WRITE message for replication protocol
				if(request.command() == "WRITE"){
					cout<<"received WRITE"<<endl;
					//locally update and send an ACK
					update_kv_store(request_str);
				}
				//this is the primary server receiving request to WRITE
				else if(request.command() == "REQUEST"){
					//locally update
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
						if(start_letter >= rowkey_range[i].first && start_letter <= rowkey_range[i].second)
						{
							key = toKey(rowkey_range[i].first, rowkey_range[i].second);
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
							//locally update
							string preprocessed_response = update_kv_store(request_str);
							request.set_preprocessed_response(preprocessed_response);
							request.SerializeToString(&new_request_str);
							//ask secondary servers to update
							update_secondary(new_request_str);
							//add the msg to holdback queue - TODO
						}
						else{
							//request primary for permission
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
			
			current_buffer = net_buffer;
			command_end_index += suffix_length;
			//Move rest of the commands to the front
			while(*command_end_index != NULL)
			{
				*current_buffer = *command_end_index;
				*command_end_index = '\0';
				current_buffer++;
				command_end_index++;
			}
			//Clear rest of the buffer
			while(*current_buffer != '\0')
			{
				*current_buffer = '\0';
				current_buffer++;
			}
		}
		// Start from the beginning (since we could've potentially moved commands if there were multiple commands)
		// There's only one command now!
		current_buffer = net_buffer;
		while(*current_buffer != NULL)
			current_buffer++;
		delete command_end_index;
	}
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
	thread checkpoint_thread(checkpoint_kvstore);

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

		client_sockets.push_back(client_socket);
		if(verbose)
			cerr<<("Connection from %s\n", inet_ntoa(clientaddr.sin_addr));
		pthread_t thread;
		
		if(pthread_create(&thread, NULL, process_client_thread, &client_sockets.back()) != 0)
		{
			cerr << "Thread creation error!\r\n";
			return -1;
		}
		client_threads.push_back(thread);
		pthread_detach(thread);	
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

	load_kvstore_from_disk();
	//create log file if it doesn't exist
	create_log_file();
	replay_log(log_file_name, meta_log_file_name);
    int isSuccess = create_server();
	return isSuccess;
}