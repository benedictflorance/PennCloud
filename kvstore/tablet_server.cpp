#include "utils/command_processor.h"
#include "utils/tools.h"
#include "utils/log.h"
#include "utils/hash.h"
#include "utils/config_processor.h"
#include "utils/update_manager.h"
#include "utils/background_daemons.h"

void *process_client_thread(void *arg);
int create_server();

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
	create_dir(log_dir);
	create_dir("checkpoints/");
	create_dir("metadata/");
	create_dir(meta_log_dir);
	create_file(log_file_name);
	create_file(meta_log_file_name);
}

void *process_client_thread(void *arg)
{	
	int client_socket = *(int*) arg;
	string response_str;

	if(verbose)
	{
		// [N] New connection (where N is the file descriptor of the connection);
		cerr<<"["<<client_socket<<"] "<<new_connection_message<<endl;
	}

	char length_buffer[BUFFER_SIZE];
	char request_buffer[BUFFER_SIZE];
	while(true)
	{	
		if(shutdown_flag)
		{
			if(verbose)
					cerr<<"["<<client_socket<<"] "<<closing_message<<endl;
			write(client_socket, shutdown_message, strlen(shutdown_message));
			close(client_socket);
			pthread_exit(NULL);
		}
		// memset(length_buffer, 0, sizeof(length_buffer));
		// memset(request_buffer, 0, sizeof(request_buffer));
		do_read(client_socket, length_buffer, 10);
		int request_length = stoi(string(length_buffer));
		cout<<"Request length is "<<request_length<<endl;
		do_read(client_socket, request_buffer, request_length);
		cout<<"Current buffer length "<<strlen(request_buffer)<<endl;
		string request_str = string(request_buffer);
		PennCloud::Request request;
		request.ParseFromString(request_str);
		PennCloud::Response response;
		string response_str;
		if(!request.has_type())
		{
			response.set_status(type_unset_message.first);
			response.set_description(type_unset_message.second);
		}
		else if (strcasecmp(request.type().c_str(), "GET") == 0){
			process_get_request(request, response);
		}
		else if (strcasecmp(request.type().c_str(), "PUT") == 0){
			update_log(log_file_name,meta_log_file_name,request.type(),request.rowkey(),request.columnkey(),request.value1(),request.value2());
			process_put_request(request, response);
		}
		else if (strcasecmp(request.type().c_str(), "CPUT") == 0){
			update_log(log_file_name,meta_log_file_name,request.type(),request.rowkey(),request.columnkey(),request.value1(),request.value2());
			process_cput_request(request, response);
		}
		else if (strcasecmp(request.type().c_str(), "DELETE") == 0){
			update_log(log_file_name,meta_log_file_name,request.type(),request.rowkey(),request.columnkey(),request.value1(),request.value2());
			process_delete_request(request, response);
		}
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
				cerr<<"["<<client_socket<<"] "<<"Client received a request type of "<<request.type()<<" a rowkey of "<<request.rowkey()<<" a columnkey of "<<request.columnkey()<<" a value1 of "<<request.value1().length()
					<<" a value2 of "<<request.value2().length()<<endl; 
				// [N] S: <text> (where <text> is a response sent by the server, and N is as above);
				cerr<<"["<<client_socket<<"] "<<"Server sent a response status of  "<<response.status()<<" response description of "<<response.description()<<" response value of "<<response.value().length()<<endl;
			}
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
	create_log_file();
	load_kvstore_from_disk();
	//create log file if it doesn't exist
	replay_log(log_file_name, meta_log_file_name);
    int isSuccess = create_server();
	return isSuccess;
}