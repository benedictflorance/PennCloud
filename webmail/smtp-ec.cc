#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <csignal>
#include <sys/socket.h>
#include <errno.h>
#include <vector>
#include <cstring>
#include <pthread.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <dirent.h>
#include <unordered_set>
#include <unordered_map>
#include <string>
#include <algorithm>
#include <chrono>
#include <ctime>    
#include <sys/file.h>
#include "mail.pb.h"
#include "../kvstore/client_wrapper.h"
using namespace std;

/*
			Global Variables
*/
int server_socket;
vector<int> client_sockets;
vector<pthread_t> client_threads;
bool verbose = false;
// Problem #1 with this approach is that new entries could be added to the array while the signal handler is already running. This can be prevented by having a global 
// (and equally volatile) "shutting down" flag that is set when the signal handler starts.
volatile bool shutdown_flag = false;
// If the server limits the length of a command, the limit should be at least 1,000 characters.
const int BUFFER_SIZE = 10000;
string mailbox_directory;
unordered_map<string, pthread_mutex_t> mailboxes;
/*
				Messages
*/
const char* new_connection_message = "New connection\r\n";
const char* closing_message = "Connection closed\r\n";
// CONNECTION ESTABLISHMENT
// 	S: 220
const char* service_ready_message = "220 localhost Service ready\r\n";
// 	F: 421
const char* shutdown_message = "421 localhost Service not available, closing transmission channel\r\n";
// HELO
// 	S: 250
const char*  helo_response_message = "250 localhost\r\n";
const char*  ok_message = "250 OK\r\n";
// 	E: 500, 501, 504, 421
const char* syntax_error_message = "500 Syntax error, command unrecognized\r\n";
const char* param_error_message = "501 Syntax error in parameters or arguments\r\n";
const char* command_incomplete_message = "504 Command parameter not implemented\r\n";
// MAIL
// 	S: 250
// 	F: 552, 451, 452
const char* storage_message = "552 Requested mail action aborted: exceeded storage allocation\r\n";
const char* local_error_message = "451 Requested action aborted: local error in processing\r\n";
const char* insufficient_storage_message = "452 Requested action not taken: insufficient system storage\r\n";
// 	E: 500, 501, 421
// RCPT
// 	S: 250, 251
const char* nonlocal_message = "251 User not local; will forward to nonlocal@localhost\r\n";
// 	F: 550, 551, 552, 553, 450, 451, 452
const char* mailbox_notfound_message = "550 Requested action not taken: mailbox unavailable [mailbox not found, no access]\r\n";
const char* nonlocal_failure_message = "551 User not local; please try nonlocal@localhost\r\n";
const char* exceeded_storage_message = "552 Requested mail action aborted: exceeded storage allocation\r\n";
const char* mailbox_notallowed_message = "553 Requested action not taken: mailbox name not allowed [mailbox syntax incorrect]\r\n";
const char* mailbox_busy_message = "450 Requested mail action not taken: mailbox unavailable [mailbox busy]\r\n";
// 	E: 500, 501, 503, 421
const char* sequence_error_message = "503 Bad sequence of commands\r\n";
// DATA
// 	I: 354 -> data -> S: 250
const char* intermediate_message = "354 Start mail input; end with <CRLF>.<CRLF>\r\n";
// 	F: 552, 554, 451, 452
const char* transaction_failure_message = "554 Transaction failed\r\n";
// 	F: 451, 554
// 	E: 500, 501, 503, 421
// RSET
// 	S: 250
// 	E: 500, 501, 504, 421     
// NOOP
// 	S: 250
// 	E: 500, 421
// QUIT
// 	S: 221
// 	E: 500       
const char* trans_closing_message = "221 localhost Service closing transmission channel\r\n";

void signal_handler(int arg)
{
	/*
	If the user terminates the server by pressing Ctrl+C, the server should write -ERR Server shutting down to each open connection and then close all open
	sockets before terminating.   
	*/
	shutdown_flag = true;
	close(server_socket);
	cerr<<endl<<shutdown_message;
	// Signal handler could simply iterate over this array, set each connection to nonblocking, write the goodbye message to it, and then close it.
	for(auto it = mailboxes.begin(); it != mailboxes.end(); it++)
		pthread_mutex_unlock(&mailboxes[it->first]);
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
/*
	HELO command is used to identify the sender-SMTP to the receiver-SMTP.  The argument field contains 
	the host name of the sender-SMTP.

	The receiver-SMTP identifies itself to the sender-SMTP in the connection greeting reply, and in the 
	response to this command.

	This command and an OK reply to it confirm that both the sender-SMTP and the receiver-SMTP are in the 
	initial state, that is, there is no transaction in progress and all state tables and buffers are cleared.
*/
string process_helo_command(int &client_socket, char* net_buffer, int full_command_length, int &sequence_phase, string &reverse_path_buffer, unordered_set<string> &forward_path_buffer, string &mail_data_buffer)
{
	int command_size = strlen("HELO ");
	string response;
	/* As described above, an OK response only serves to confirm that the server is in the initial state and 
	hence multiple following HELO commands are acceptable. Thus, in your scenario, if the server is still in the initial 
	state when the second HELO is issued, then the server can accept it. (Note that an actual mail transaction only begins 
	when a MAIL FROM is issued.) */
	if(sequence_phase != -1 && sequence_phase != 0) // Already past the init state
		response = sequence_error_message;
	else if(strlen(net_buffer) == command_size)  // Host name of sender-SMTP not present
		response = param_error_message;
	else
	{
		string hostname(net_buffer + command_size, full_command_length - command_size);
		hostname = hostname.substr(0, hostname.length() - 2); // Remove \r\n
		if (hostname.find(' ') != std::string::npos || hostname.empty())
			response = param_error_message;
		else
		{
			response = helo_response_message;
			sequence_phase = 0; // Both the sender-SMTP and the receiver-SMTP are in the initial state
		}
		// All state tables and buffers are cleared
		reverse_path_buffer.clear();
		forward_path_buffer.clear();
		mail_data_buffer.clear();
	}
	write(client_socket, response.c_str(), strlen(response.c_str()));
	return response;
}
/*
	MAIL FROM: command is used to initiate a mail transaction in which the mail data is delivered to one or more mailboxes.  
	The argument field contains a reverse-path.

	The reverse-path consists of an optional list of hosts and the sender mailbox. This command clears the reverse-path buffer, 
	the forward-path buffer, and the mail data buffer; and inserts the reverse-path information from this command into the
	reverse-path buffer.
*/

string process_mail_from_command(int &client_socket, char* net_buffer, int full_command_length, int &sequence_phase, string &reverse_path_buffer, unordered_set<string> &forward_path_buffer, string &mail_data_buffer)
{
	int command_size = strlen("MAIL FROM:<");
	string response;
	if(sequence_phase != 0) // Already past the mail state or before init state
		response = sequence_error_message;
	else
	{
		string email(net_buffer + command_size, full_command_length - command_size);
		email = email.substr(0, email.length() - 3); // Remove >\r\n
		if(email.length() < 3 || count(email.begin(), email.end(), '@') != 1 || 
			email.find('@') == email.length() - 1 || email.find('@') == std::string::npos
			|| email.find(' ') != std::string::npos) // Email should atleast have letter@letter
			response = mailbox_notallowed_message;
		else
		{
			response = ok_message;
			sequence_phase = 1;
			// All buffers are cleared
			reverse_path_buffer.clear();
			forward_path_buffer.clear();
			mail_data_buffer.clear();
			// inserts the reverse-path information
			reverse_path_buffer = email;
		}
	}
	write(client_socket, response.c_str(), strlen(response.c_str()));
	return response;
}
/*
	RCPT TO command is used to identify an individual recipient of the mail data; multiple recipients are specified by multiple
	use of this command.

	The forward-path consists of an optional list of hosts and a required destination mailbox. If the receiver-SMTP does not 
	implement the relay function it may user the same reply it would for an unknown local user (550).

	This command causes its forward-path argument to be appended to the forward-path buffer.
*/
string process_rcpt_to_command(int &client_socket, char* net_buffer, int full_command_length, int &sequence_phase, unordered_set<string> &forward_path_buffer)
{
	int command_size = strlen("RCPT TO:<");
	string response;
	if(sequence_phase != 1 && sequence_phase != 2) // Can accept command only in mail from phase or rcpt to phase
		response = sequence_error_message;
	else
	{
		string email(net_buffer + command_size, full_command_length - command_size);
		email = email.substr(0, email.length() - 3); // Remove >\r\n
		int at_index = email.find('@');
		if(email.length() < 3 || count(email.begin(), email.end(), '@') != 1 || 
			email.find('@') == email.length() - 1 || email.find('@') == std::string::npos
			|| email.find(' ') != std::string::npos) // Email should atleast have letter@letter
			response = param_error_message;
		else
		{
			string username = email.substr(0, at_index-0);
			string hostname = email.substr(at_index+1, email.length()-at_index-1);
			if(strcasecmp(hostname.c_str(), "localhost") != 0)
			{
				response = ok_message;
                sequence_phase = 2;
                // forward-path argument to be appended to the forward-path buffer
                forward_path_buffer.insert(email);
			}
			else
			{
                if(mailboxes.find(username + ".mbox") == mailboxes.end())
				    response = mailbox_notfound_message;
                else
                {
                    response = ok_message;
                    sequence_phase = 2;
                    // forward-path argument to be appended to the forward-path buffer
                    forward_path_buffer.insert(username + ".mbox");
                }
			}
		}
	}
	write(client_socket, response.c_str(), strlen(response.c_str()));
	return response;
}
/*
	The receiver treats the lines following the DATA command as mail data from the sender.  This command causes the mail data
    from this command to be appended to the mail data buffer.The mail data may contain any of the 128 ASCII character codes.

	The mail data is terminated by a line containing only a period, that is the character sequence "<CRLF>.<CRLF>". This is 
	the end of mail data indication.

	The end of mail data indication requires that the receiver must now process the stored mail transaction information.
    This processing consumes the information in the reverse-path buffer, the forward-path buffer, and the mail data buffer,
    and on the completion of this command these buffers are cleared.  If the processing is successful the receiver must
    send an OK reply.  If the processing fails completely the receiver must send a failure reply.	

	When the receiver-SMTP makes the "final delivery" of a message it inserts at the beginning of the mail data a
	The return path line preserves the information in the <reverse-path> from the MAIL command.
    Here, final delivery means the message leaves the SMTP world. the final mail data will begin with a  return path line, 
	followed by one or more time stamp lines.  These lines will be followed by the mail data header and body.

	If after accepting several recipients and the mail data, the receiver-SMTP finds that the mail data can be successfully
    delivered to some of the recipients, but it cannot be to others (for example, due to mailbox space allocation
    problems).  In such a situation, the response to the DATA command must be an OK reply. 
*/
string process_data_command(int &client_socket, char* net_buffer, int full_command_length, int &sequence_phase, string &reverse_path_buffer, unordered_set<string> &forward_path_buffer, string &mail_data_buffer, FILE* &fout)
{
	string response;
	if(sequence_phase != 2 && sequence_phase != 3) // Can accept command only in mail from phase or rcpt to phase
		response = sequence_error_message;
	else if(sequence_phase == 2)
	{
		sequence_phase = 3;
		response = intermediate_message;
	}
	else if(strcmp(string(net_buffer, full_command_length).c_str(), ".\r\n") != 0)
	{
		string message(net_buffer, full_command_length);
		mail_data_buffer += message;
		response = "";
	}
	else
	{
		/* The file should follow the mbox format - that is, each email should start with a line From <email> <date> (Example: 
		From <linhphan@localhost> Mon Feb 05 23:00:00 2018), and after that, the exact text that was sent by the client (but 
		without the final dot). */
		auto time_now = chrono::system_clock::to_time_t(chrono::system_clock::now());
		string time_string = string(ctime(&time_now));
		string header = "From <" + reverse_path_buffer + "> " + time_string.substr(0, time_string.size() - 1) + "\r\n";
		bool wrote_atleast_one_email = false; // Set to true only if atleast mail to one recipient succeeds!
		for(auto mailbox: forward_path_buffer)
		{
            if(mailboxes.find(mailbox) != mailboxes.end())
            {
                pthread_mutex_lock(&mailboxes[mailbox]);
                fout = fopen((mailbox_directory + "/" + mailbox).c_str(), "r");
                int flock_return = flock(fileno(fout), LOCK_EX | LOCK_NB);
                if(flock_return == 0)
                {
					string new_mail = mail_data_buffer;
					string original_mail = kvstore.get(mailbox, "__.mbox");
					string total_mail = original_mail + new_mail;
                    ofstream mailstream;
					kvstore.put(mailbox, "__.mbox", total_mail);
                    wrote_atleast_one_email = true;
                }
                if(fout)
                    fclose(fout);
                fout = NULL;
                pthread_mutex_unlock(&mailboxes[mailbox]);
            }
            else
            {
                string mqueue = "mqueue.txt";
                pthread_mutex_lock(&mailboxes[mqueue]);
                fout = fopen((mailbox_directory + "/" + mqueue).c_str(), "r");
                int flock_return = flock(fileno(fout), LOCK_EX | LOCK_NB);
                if(flock_return == 0)
                {
                    ofstream mailstream;
                    mailstream.open(mailbox_directory + "/" + mqueue, ios::app);
                    mailstream<<"<BEGINMAIL>\r\n";
                    mailstream<<reverse_path_buffer + "\r\n";
                    mailstream<<mailbox + "\r\n";
                    mailstream<<mail_data_buffer;
                    mailstream.close();
                    wrote_atleast_one_email = true;
                }
                if(fout)
                    fclose(fout);
                fout = NULL;
                pthread_mutex_unlock(&mailboxes[mqueue]);                
            }
		}
		if(wrote_atleast_one_email)
			response = ok_message;
		else
			response = mailbox_busy_message;
		// All buffers are cleared
		reverse_path_buffer.clear();
		forward_path_buffer.clear();
		mail_data_buffer.clear();
		sequence_phase = 0;
	}
	if(response != "")
		write(client_socket, response.c_str(), strlen(response.c_str()));
	return response;
}
void *process_client_thread(void *arg)
{	
	/*
	When a client connects, the server should send a simple greeting message 
	The greeting message should have the form +OK Server ready (Author: Linh Thi Xuan Phan /
	linhphan), except that you should fill in your own name and SEAS login.
	*/
	int client_socket = *(int*) arg;
	write(client_socket, service_ready_message, strlen(service_ready_message));
	if(verbose)
	{
		// [N] New connection (where N is the file descriptor of the connection);
		cerr<<"["<<client_socket<<"] "<<new_connection_message<<endl;
		cerr<<"["<<client_socket<<"] "<<"S: "<<service_ready_message<<endl;
	}

	char net_buffer[BUFFER_SIZE];
	char* current_buffer = net_buffer;
	bool closeConnection = false;
	int sequence_phase = -1; // Transaction hasn't begun
	/*
	-1: Transaction hasn't begun
	0: Initialization/HELO received
	1: MAIL FROM received
	2: RCPT TO received
	3: DATA command received
	4: DATA message received
	*/
	string reverse_path_buffer;
	// The server should accept both commands, and it may simply write the email once or twice; either is fine. 
	// We use unordered set to prevent duplicates here!
	unordered_set<string> forward_path_buffer; 
	string mail_data_buffer;
	FILE* fout;
	while(true)
	{	
		char *command_end_index;
		// Only read upto end of the allowed buffer size
		int client_shutdown = read(client_socket, current_buffer, BUFFER_SIZE-strlen(net_buffer));
		// Then whatever thread is adding values to the array could check that flag after adding a new value, and close the connection immediately if the flag is set. 
		// Thus, once the flag is set, any new connections that may be added to the array will immediately be closed, so the signal handler won't need to worry about them.
		if(shutdown_flag || (client_shutdown == 0))
		{	
			if(verbose)
		    	cerr<<"["<<client_socket<<"] "<<closing_message<<endl;
			write(client_socket, shutdown_message, strlen(shutdown_message));
			close(client_socket);
			if(fout != NULL)
				fclose(fout);
			for(auto mailbox: forward_path_buffer)
				pthread_mutex_unlock(&mailboxes[mailbox]);
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
			const int command_length = 5, suffix_length = 2; // "\r\n" is 2 letters
			int full_command_length = command_end_index + suffix_length - net_buffer;
			char* command = new char[command_length]; 
			strncpy(command, net_buffer, command_length);
			command[command_length] = '\0'; 
			string response;
			//  Commands should be treated as case-insensitive - strcasecmp
			// DATA, which is followed by the text of the email and then a dot (.) on a line by itself
			if(sequence_phase == 3 || ((strcasecmp(command, "DATA\r") == 0) && (strncasecmp(net_buffer + command_length, "\n", strlen("\n")) == 0))) // If sequence phase is 3, we are in the process of sending data
				response = process_data_command(client_socket, net_buffer, full_command_length, sequence_phase, reverse_path_buffer, forward_path_buffer, mail_data_buffer, fout);
			// HELO <domain>, which starts a connection;
			else if(strcasecmp(command, "HELO ") == 0)
				response = process_helo_command(client_socket, net_buffer, full_command_length, sequence_phase, reverse_path_buffer, forward_path_buffer, mail_data_buffer);
			// MAIL FROM:, which tells the server who the sender of the email is;
			else if((strcasecmp(command, "MAIL ") == 0) && (strncasecmp(net_buffer + command_length, "FROM:<", strlen("FROM:<")) == 0) 
					&& (strncasecmp(net_buffer + full_command_length - suffix_length - 1, ">", strlen(">")) == 0))
				response = process_mail_from_command(client_socket, net_buffer, full_command_length, sequence_phase, reverse_path_buffer, forward_path_buffer, mail_data_buffer);
			// RCPT TO:, which specifies the recipient;
			else if((strcasecmp(command, "RCPT ") == 0) && (strncasecmp(net_buffer + command_length, "TO:<", strlen("TO:<")) == 0)
					&& (strncasecmp(net_buffer + full_command_length - suffix_length - 1, ">", strlen(">")) == 0))
				response = process_rcpt_to_command(client_socket, net_buffer, full_command_length, sequence_phase, forward_path_buffer);
			/*
			QUIT command specifies that the receiver must send an OK reply, and then close the transmission channel.

			The receiver should not close the transmission channel until it receives and replies to a QUIT command (even if there was
			an error).  The sender should not close the transmission channel until it send a QUIT command and receives the reply
			(even if there was an error response to a previous command). If the connection is closed prematurely the receiver should
			act as if a RSET command had been received (canceling anypending transaction, but not undoing any previously completed 
			transaction), the sender should act as if the	command or transaction in progress had received a temporary	error (4xx).
			*/
			else if((strcasecmp(command, "QUIT\r") == 0))
			{
				closeConnection = true;
				response = trans_closing_message;
				write(client_socket, response.c_str(), strlen(response.c_str()));
			}
			/*
			This command specifies that the current mail transaction is to be aborted.  Any stored sender, recipients, and mail data must be discarded, and all buffers and state tables cleared. The receiver must send an OK reply.
			*/
			else if((strcasecmp(command, "RSET\r") == 0))
			{	
				if(sequence_phase == -1)
					response = sequence_error_message;
				else
				{
					sequence_phase = 0;
					// All buffers are cleared
					reverse_path_buffer.clear();
					forward_path_buffer.clear();
					mail_data_buffer.clear();
					response = ok_message;
				}
				write(client_socket, response.c_str(), strlen(response.c_str()));
			}
			/*
			NOOP command does not affect any parameters or previously entered commands.  It specifies no action other than that
            the receiver send an OK reply.

            This command has no effect on any of the reverse-path buffer, the forward-path buffer, or the mail data buffer.
			*/
			else if((strcasecmp(command, "NOOP\r") == 0))
			{
				if(sequence_phase == -1)
					response = sequence_error_message;
				else
					response = ok_message;
				write(client_socket, response.c_str(), strlen(response.c_str()));
			}
			// For anything else (in particular, EHLO), your server should return an error code.
			else
			{
				response = syntax_error_message;
				write(client_socket, response.c_str(), strlen(response.c_str()));
			}
			if(verbose)
				{
					// [N] C: <text> (where <text> is a command received from the client and N is as above);
					cerr<<"["<<client_socket<<"] "<<"C: "<<string(net_buffer, full_command_length)<<endl; 
					// [N] S: <text> (where <text> is a response sent by the server, and N is as above);
					if(response != "") 
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
			delete[] command;
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

void create_mailbox_mutexes()
{
	DIR* dir;
	struct dirent *ent;
	if ((dir = opendir (mailbox_directory.c_str())) != NULL) 
	{
		while ((ent = readdir (dir)) != NULL) 
		{
			string dirname = string(ent->d_name);
			if(dirname != "." && dirname != ".." && mailboxes.find(dirname) == mailboxes.end())
			{	
				pthread_mutex_t mutex;
				mailboxes[dirname] = mutex;
				if ((pthread_mutex_init(&mailboxes[dirname], NULL)) != 0) 
				{
					cerr<<"Mutex init has failed!\r\n";
					exit(-1);
				}	
			}
		}
		closedir (dir);
	} 
	else 
	{
		cerr<<"Directory doesn't exist!\r\n";
		exit(-1);
	}
}
int create_server(int port)
{
	/*
	If the user terminates the server by pressing Ctrl+C, the server should write -ERR Server shutting down to each open connection and then close all open
	sockets before terminating.   
	*/
	signal(SIGINT, signal_handler);
	create_mailbox_mutexes();
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
    int option, port = 2500;
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
	// On the command line, your server should accept the name of a directory that contains the mailboxes of the
	// local users. Each file in this directory should store the email for one local user; a file with the name
	// user.mbox would contain the messages for the user with the email address user@localhost.
	if(argc == optind)
	{
		cerr<<"Mailbox directory not provided!";
		return -1;
	}
	mailbox_directory = argv[optind];
	int isSuccess = create_server(port);
	return isSuccess;
}