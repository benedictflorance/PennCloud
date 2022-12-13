#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <iostream>
#include <csignal>
#include <sys/socket.h>
#include <vector>
#include <cstring>
#include <pthread.h>
#include <arpa/inet.h>
#include <fcntl.h>
using namespace std;

int server_socket;
vector<int> client_sockets;
vector<pthread_t> client_threads;
bool verbose = false;
// Problem #1 with this approach is that new entries could be added to the array while the signal handler is already running. This can be prevented by having a global 
// (and equally volatile) "shutting down" flag that is set when the signal handler starts.
volatile bool shutdown_flag = false;
// If the server limits the length of a command, the limit should be at least 1,000 characters.
const int BUFFER_SIZE = 1000;

const char* shutdown_message = "-ERR Server shutting down\r\n";
const char* greeting_message = "+OK Server ready (Author: Benedict Florance Arockiaraj / benarock)\r\n";
const char* new_connection_message = "New connection\r\n";
const char* ok_message = "+OK ";
const char* goodbye_message ="+OK Goodbye!\r\n";
const char* unknown_message = "-ERR Unknown command\r\n";
const char* closing_message = "Connection closed\r\n";

void signal_handler(int arg)
{
	/*
	If the user terminates the server by pressing Ctrl+C, the server should write -ERR Server shutting down to each open connection and then close all open
	sockets before terminating.   
	*/
	shutdown_flag = true;
	close(server_socket);
	cout<<endl<<shutdown_message;
	// Signal handler could simply iterate over this array, set each connection to nonblocking, write the goodbye message to it, and then close it.
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
	client_sockets.clear();
	client_threads.clear();
	exit(-1);
}

void *process_client_thread(void *arg)
{	
	/*
	When a client connects, the server should send a simple greeting message 
	The greeting message should have the form +OK Server ready (Author: Linh Thi Xuan Phan /
	linhphan), except that you should fill in your own name and SEAS login.
	*/
	int client_socket = *(int*) arg;
	write(client_socket, greeting_message, strlen(greeting_message));
	if(verbose)
	{
		// [N] New connection (where N is the file descriptor of the connection);
		cerr<<"["<<client_socket<<"] "<<new_connection_message<<endl;
		cerr<<"["<<client_socket<<"] "<<"S: "<<greeting_message<<endl;
	}
	char net_buffer[BUFFER_SIZE];
	memset(net_buffer, 0, sizeof(net_buffer));
	char* current_buffer = net_buffer;
	bool closeConnection = false;
	while(true)
	{	
		char *command_end_index;
		// Only read upto end of the allowed buffer size
		read(client_socket, current_buffer, BUFFER_SIZE-strlen(net_buffer));
		// Then whatever thread is adding values to the array could check that flag after adding a new value, and close the connection immediately if the flag is set. 
		// Thus, once the flag is set, any new connections that may be added to the array will immediately be closed, so the signal handler won't need to worry about them.
		if(shutdown_flag)
		{
			write(client_socket, shutdown_message, strlen(shutdown_message));
			close(client_socket);
			pthread_exit(NULL);
		}
		/*
		Each command is terminated by a carriage return (\r) followed by a linefeed character (\n).
		To handle this, your server should maintain a buffer for each connection; when new data arrives, it should
		be added to the corresponding buffer, and the server should then check whether it has received a full line.
		If so, it should process the corresponding command, remove the line from the buffer, and repeat until the
		buffer no longer contains a full line.
		*/
		while((command_end_index = strstr(net_buffer, "\r\n")) != NULL)
		{
			const int command_length = 5, suffix_length = 2, ok_length = 4; // ECHO or QUIT is 4 letters, "\r\n" is 2 letters, "+OK " is four letters
			int full_command_length = command_end_index + suffix_length - net_buffer;
			char* command = new char[command_length]; 
			char* full_command = new char[full_command_length];
			strncpy(command, net_buffer, command_length);
			strncpy(full_command, net_buffer, full_command_length);
			command[command_length] = '\0'; 
			full_command[full_command_length] = '\0'; 
			char response[BUFFER_SIZE];
			//  Commands should be treated as case-insensitive - strcasecmp
			// ECHO <text>, to which the server should respond +OK <text> to the client;
			if(strcasecmp(command, "ECHO ") == 0 || ((strcasecmp(command, "ECHO\r") == 0) && (strncmp(net_buffer + command_length, "\n", 1) == 0))) // Second part is to just handle ECHO<CR><LF>
			{
				strcpy(response, ok_message);
				// +1 for space between command and text
				char* prefix_length = net_buffer + command_length;
				unsigned int response_length = (command_end_index + suffix_length) - prefix_length;
				strncpy(&response[ok_length], prefix_length, response_length); 
				response[ok_length + response_length] = '\0';
				write(client_socket, response, strlen(response));
			}
			// QUIT, to which the server should respond +OK Goodbye! to the client and then close the connection
			// Second part to handle Ed comment: For the purpose of HW1MS1, a QUIT followed by one or more space and followed by some data may be accepted. 
			// That is "QUIT abc <CR><LF>" would be considered as valid. 
			else if((strcasecmp(command, "QUIT\r") == 0) || (strcasecmp(command, "QUIT ") == 0))
			{	
				strcpy(response, goodbye_message);
				write(client_socket, response, strlen(response));
				closeConnection = true;
			}
			else
			{
				strcpy(response, unknown_message);
				write(client_socket, response, strlen(response));
			}
			if(verbose)
				{
					// [N] C: <text> (where <text> is a command received from the client and N is as above);
					cerr<<"["<<client_socket<<"] "<<"C: "<<full_command<<endl; 
					// [N] S: <text> (where <text> is a response sent by the server, and N is as above); 
					cerr<<"["<<client_socket<<"] "<<"S: "<<response<<endl;
				}
			if(closeConnection)
				break;
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
			delete command;
			delete full_command;
		}
		if(closeConnection)
			break;
		// Start from the beginning (since we could've potentially moved commands if there were multiple commands)
		// There's only one command now!
		current_buffer = net_buffer;
		while(*current_buffer != NULL)
			current_buffer++;
		delete command_end_index;
	}
	close(client_socket);
	if(verbose)
		cerr<<"["<<client_socket<<"] "<<closing_message<<endl;
	// The server should also properly clean up its resources, e.g., by terminating pthreads when their connection closes;
	pthread_exit(NULL);
}

int create_server(int port)
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
	const int opt = 1;
	if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) 
	{
		cerr << "Set socket options error!\r\n";
		return -1;
	}
	struct sockaddr_in server_address;
	bzero(&server_address, sizeof(server_address)); 
	server_address.sin_family = AF_INET;
	server_address.sin_addr.s_addr = INADDR_ANY;
	server_address.sin_port = htons(port);
	if (bind(server_socket, (struct sockaddr*)&server_address, sizeof(server_address)) < 0) 
	{
		cerr << "Binding error!\r\n";
		return -1;
	}
	// Max queue length for pending connections
	// The default backlog is 128, so you can set any number below that — e.g., say 100?
	if (listen(server_socket, 100) < 0) {
		cerr << "Listen error!\r\n";
		return -1;
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
		client_sockets.push_back(client_socket);
		if(verbose)
			cerr<<("Connection from %s\n", inet_ntoa(clientaddr.sin_addr));
		// Start a new pthread, which should read commands from the connection and process them until the user closes the connection
		pthread_t thread;
		if(pthread_create(&thread, NULL, process_client_thread, &client_sockets.back()) != 0)
		{
			cerr << "Thread creation error!\r\n";
			return -1;
		}
		client_threads.push_back(thread);
		// Since the thread is detached now, its resources are automatically released back to the system upon termination
		pthread_detach(thread);
	}
	return 0;
}

int main(int argc, char *argv[])
{
    /* 
    The server should support three command-line options: -p <portno>, -a, and -v. If the -p option is
    given, the server should accept connections on the specified port; otherwise it should use port 10000. 
    If the -a option is given, the server should output your full name and SEAS login to stderr, and then 
    exit. If the -v option is given, the server should print debug output to stderr.  
    */
    int option, port = 10000;
    while((option = getopt(argc, argv, "p:av")) != -1)
	{
		switch(option)
		{
			case 'p':
					port = atoi(optarg);
					break;
			case 'a':
					cerr << "Benedict Florance Arockiaraj (benarock)\r\n";
					exit(0);
			case 'v':
					verbose = true;
					break;
			default:
					// Invalid option
					cerr<<"Option not recognized!\r\n";
					return -1;
		}
	}
	int isSuccess = create_server(port);
	return isSuccess;
}