#include "request.pb.h"
#include "response.pb.h"
#include "utils/command_processor.h"
#include "utils/config_processor.h"
#include "utils/update_manager.h"

void *process_client_thread(void *arg);
int create_server();

void *process_client_thread(void *arg)
{	
	int client_socket = *(int*) arg;
	write(client_socket, service_ready_message, strlen(service_ready_message));
	if(verbose)
	{
		// [N] New connection (where N is the file descriptor of the connection);
		cerr<<"["<<client_socket<<"] "<<new_connection_message<<endl;
		cerr<<"["<<client_socket<<"] "<<"S: "<<service_ready_message<<endl;
	}

	char net_buffer[BUFFER_SIZE];
	memset(net_buffer, 0, sizeof(net_buffer));
	char* current_buffer = net_buffer;

	while(true)
	{	
		char *command_end_index;
		read(client_socket, current_buffer, BUFFER_SIZE-strlen(net_buffer));
		if(shutdown_flag)
		{
			if(verbose)
					cerr<<"["<<client_socket<<"] "<<closing_message<<endl;
					write(client_socket, shutdown_message, strlen(shutdown_message));
					close(client_socket);
		}
		while((command_end_index = strstr(net_buffer, "\r\n")) != NULL)
		{
			int full_command_length = command_end_index + suffix_length - net_buffer;
			string request_str = string(net_buffer, full_command_length);
			PennCloud::Request request;
			request.ParseFromString(request_str);
			PennCloud::Response response;
			string response_str;
			if(!request.has_type())
			{
				response.set_status(type_unset_message.first);
				response.set_description(type_unset_message.second);
			}
			else if (strcasecmp(request.type().c_str(), "GET") == 0)
				process_get_request(request, response);
			else if (strcasecmp(request.type().c_str(), "PUT") == 0)
				process_put_request(request, response);
			else if (strcasecmp(request.type().c_str(), "CPUT") == 0)
				process_cput_request(request, response);
			else if (strcasecmp(request.type().c_str(), "DELETE") == 0)
				process_delete_request(request, response);
			else
			{
				response.set_status(unrecognized_command_message.first);
				response.set_description(unrecognized_command_message.second);
			}		
			response.SerializeToString(&response_str);								
			write(client_socket, response_str.c_str(), strlen(response_str.c_str()));
			if(verbose)
				{
					// [N] C: <text> (where <text> is a command received from the client and N is as above);
					cerr<<"["<<client_socket<<"] "<<"Client received a request type of "<<request.type()<<" a rowkey of "<<request.rowkey()<<" a columnkey of "<<request.columnkey()<<" a value1 of "<<request.value1()
						<<" a value2 of "<<request.value2()<<endl; 
					// [N] S: <text> (where <text> is a response sent by the server, and N is as above);
					cerr<<"["<<client_socket<<"] "<<"Server sent a response status of  "<<response.status()<<" response description of "<<response.description()<<" response value of "<<response.value()<<endl;
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

//placeholder for sending heartbeats
void send_heartbeat(){
	//connect with master
	int sockfd = socket(PF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) 
    {
        if(verbose)
            cerr<<"Unable to create socket"<<endl;
    }
    connect(sockfd, (struct sockaddr*)& master_address, sizeof(master_address));

	//use update manager library
	while(true){
		auto t = UpdateManager::start();
		this_thread::sleep_for(1s);

		//send ALIVE command at fixed intervals
		string alive = "ALIVE\r\n";
		// if(verbose)
		// 	cout<<"Sending Alive message to the master"<<endl;
		write(sockfd, alive.c_str(), strlen(alive.c_str()));
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
    int isSuccess = create_server();
	return isSuccess;
}