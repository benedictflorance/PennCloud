#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <csignal>
#include <sys/socket.h>
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
#include <openssl/md5.h>
#include <thread>
#include <sys/file.h>
#include "../http/local_test.hpp"

using namespace std;
using namespace std::this_thread;     
using namespace std::chrono_literals; 

class EMail
{
  public:
    string UIDL;
    int size;
    int message_start;
    int header_start;
    bool is_deleted;
    EMail(string UIDL, int size, int message_start, int header_start, bool is_deleted) : UIDL(UIDL), size(size), message_start(message_start), header_start(header_start), is_deleted(is_deleted) {}
};

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
const int command_length = 5, suffix_length = 2; // "\r\n" is 2 letters
/*
				Messages
*/
const char* new_connection_message = "New connection\r\n";
const char* closing_message = "Connection closed\r\n";
const char* service_ready_message = "+OK POP3 ready [localhost]\r\n";
const char* shutdown_message = "+OK localhost POP3 server signing off (maildrop empty)\r\n";
const char* unknown_message = "-ERR Unsupported command\r\n";
const char* bad_sequence_message = "-ERR Bad sequence of commands\r\n";
const char* valid_mailbox_message = "+OK Given mailbox is valid\r\n";
const char* invalid_mailbox_message = "-ERR Mailbox not found for the given user\r\n";
const char* wrong_password_message = "-ERR Invalid password\r\n";
const char* correct_password_message = "+OK Maildrop locked and ready\r\n";
const char* uidl_message = "+OK Unique-ID listing follows\r\n";
const char* invalid_message = "-ERR No such message\r\n";
const char* invalid_argument_message = "-ERR Invalid argument\r\n";
const char* deleted_message = "-ERR Message already deleted\r\n";
const char* ok_message ="+OK\r\n";
const char* locked_mailbox_message = "-ERR Unable to acquire mailbox\r\n";

void computeDigest(char *data, int dataLengthBytes, unsigned char *digestBuffer)
{
  /* The digest will be written to digestBuffer, which must be at least MD5_DIGEST_LENGTH bytes long */

  MD5_CTX c;
  MD5_Init(&c);
  MD5_Update(&c, data, dataLengthBytes);
  MD5_Final(digestBuffer, &c);
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
string process_user_command(int &client_socket, char* net_buffer, int full_command_length, string &username, float &sequence_phase)
{
  string response;
  // may only be given in the AUTHORIZATION state after the POP3 greeting or after an unsuccessful USER or PASS command 
  // i.e., sequence phase should be zero
  if(sequence_phase > 0)
    response = bad_sequence_message;
  else
  {
    // a string identifying a mailbox (required), which is of significance ONLY to the server
    string username_arg(net_buffer + command_length, full_command_length - command_length);
    username_arg = username_arg.substr(0, username_arg.length() - 2); // Remove \r\n
    std::string mbox_name = username_arg += ".mbox";
    std::string box = kvstore.get(username_arg, mbox_name);
    if(box == "")
      response = invalid_mailbox_message;
    else
    {
      sequence_phase = 0.5;
      username = username_arg;
      response = valid_mailbox_message;
    }
  }
  write(client_socket, response.c_str(), strlen(response.c_str()));
  return response;
}

string compute_hash(string header, string email)
{
  unsigned char* digest = new unsigned char[MD5_DIGEST_LENGTH];
  string full_email_string = header + email;
  char full_email[full_email_string.length()];
  strcpy(full_email, full_email_string.c_str());
  computeDigest(full_email, strlen(full_email), digest);
  string hash;
  hash.reserve(32);
  for(int i = 0; i < 16; i++)
  {
    hash += "0123456789ABCDEF"[digest[i] / 16];
    hash += "0123456789ABCDEF"[digest[i] % 16];          
  }
  delete digest;
  return hash;
}
void load_user_mailbox(string &username, vector<EMail> &emails)
{
  string box;
  box = kvstore.get(mailbox, "__.mbox")
  stringstream strm(box);
  string email, header, line, from_pattern = "From ";
  int email_start, header_start, current_start = 0;
  bool first_message = true;
  // File read: line ends with \n, but for socket writing we'll have to change this to \r\n
  while(getline(strm, line))
  {
    if(line.substr(0, from_pattern.length()) == from_pattern)
    {
      if(first_message)
        first_message = false;
      else
      {
        string hash = compute_hash(header, email);
        emails.push_back(EMail(hash, email.length(), email_start, header_start, false));
      }
      email = "";
      header = line.substr(0, line.size() - 1) + "\r\n";
      email_start = mailstream.tellg();
      header_start = current_start;
    }
    else
    {
      line = line.substr(0, line.size() - 1) + "\r\n";
      email += line;
      current_start = mailstream.tellg();
    }
  }
  mailstream.close();
  if(!first_message)
    emails.push_back(EMail(compute_hash(header, email), email.length(), email_start, header_start, false));
}
/*
When the client issues the PASS command, the POP3 server uses the argument pair from the USER and PASS commands to
determine if the client should be given access to the appropriate maildrop.

Since the PASS command has exactly one argument, a POP3 server may treat spaces in the argument as part of the
password, instead of as argument separators.
*/
string process_pass_command(int &client_socket, char* net_buffer,  int full_command_length, string &username, float &sequence_phase, vector<EMail> &emails, FILE* &fout)
{
  // may only be given in the AUTHORIZATION state immediately after a successful USER command i.e., sequence phase is 0.5
  string response;
  if(sequence_phase != 0.5)
    response = bad_sequence_message;
  else
  {
    string password(net_buffer + command_length, full_command_length - command_length);
    password = password.substr(0, password.length() - 2); // Remove \r\n
    if(password != "cis505")
    {
      username.clear();
      sequence_phase = 0;
      response = wrong_password_message;
    }
    else
    {
      sequence_phase = 1;
      pthread_mutex_lock(&mailboxes[username]);
      fout = fopen((mailbox_directory + "/" + username).c_str(), "rw");
      int flock_return = flock(fileno(fout), LOCK_EX | LOCK_NB);
      if(flock_return == 0)
      {
        load_user_mailbox(username, emails);
        response = correct_password_message;
      }
      else
      {
        sequence_phase = 0;
        response = locked_mailbox_message;
      }
    }
    
  }
  write(client_socket, response.c_str(), strlen(response.c_str()));
  return response;
}
/*
  The POP3 server issues a positive response with a line containing information for the maildrop.  This line is
  called a "drop listing" for that maildrop.

  In order to simplify parsing, all POP3 servers are required to use a certain format for drop listings.  The
  positive response consists of "+OK" followed by a single space, the number of messages in the maildrop, a single
  space, and the size of the maildrop in octets.  This memo makes no requirement on what follows the maildrop size.
  Minimal implementations should just end that line of the response with a CRLF pair.  More advanced implementations
  may include other information.
*/
string process_stat_command(int &client_socket, float &sequence_phase, vector<EMail> &emails)
{
  string response;
  // may only be given in the TRANSACTION state
  if(sequence_phase != 1)
    response = bad_sequence_message;
  else
  {
    int message_count = 0, maildrop_size = 0;
    for(auto &email : emails)
    {
      if(!email.is_deleted)
      {
        message_count++;
        maildrop_size += email.size;
      }
    }
    response = "+OK " + to_string(message_count) + " " + to_string(maildrop_size) + "\r\n";
  }
  write(client_socket, response.c_str(), strlen(response.c_str()));
  return response;
}
/*
  If an argument was given and the POP3 server issues a positive response with a line containing information for that message.
  This line is called a "unique-id listing" for that message.

  If no argument was given and the POP3 server issues a positive response, then the response given is multi-line.  After the
  initial +OK, for each message in the maildrop, the POP3 server responds with a line containing information for that message.
  This line is called a "unique-id listing" for that message.

  In order to simplify parsing, all POP3 servers are required to use a certain format for unique-id listings.  A unique-id
  listing consists of the message-number of the message, followed by a single space and the unique-id of the message.
  No information follows the unique-id in the unique-id listing.

  The unique-id of a message is an arbitrary server-determined string, consisting of one to 70 characters in the range 0x21
  to 0x7E, which uniquely identifies a message within a maildrop and which persists across sessions.  This
  persistence is required even if a session ends without entering the UPDATE state.  The server should never reuse an
  unique-id in a given maildrop, for as long as the entity using the unique-id exists. Note that messages marked as deleted are not listed.

  While it is generally preferable for server implementations to store arbitrarily assigned unique-ids in the maildrop, 
  this specification is intended to permit unique-ids to be calculated as a hash of the message.  Clients should be able
  to handle a situation where two identical copies of a message in a maildrop have the same unique-id.
*/
string process_uidl_command(int &client_socket, char* net_buffer, int full_command_length, float &sequence_phase, vector<EMail> &emails)
{
  string response;
  // may only be given in the TRANSACTION state
  if(sequence_phase != 1)
    response = bad_sequence_message;
  else
  {
    // a message-number (optional), which, if present, may NOT refer to a message marked as deleted
    string msg(net_buffer + command_length, full_command_length - command_length);
    if(msg == "\n") // No arguments
    {
      response = uidl_message;
      for(int i = 0; i < emails.size(); i++)
	    {
        if(!emails[i].is_deleted)
          response += to_string(i + 1) + " " + emails[i].UIDL + "\r\n";
	    }
      response += ".\r\n";
    }
    else
    {
      msg = msg.substr(0, msg.length() - 2); // Remove \r\n
      try
      {
        int msg_index = stoi(msg);
        if(msg_index < 1 or msg_index > emails.size())
          response = invalid_message;
        else
        {
          if(!emails[msg_index - 1].is_deleted)
            response = "+OK " + to_string(msg_index) + " " + emails[msg_index - 1].UIDL + "\r\n";
          else
            response = deleted_message;
        }
      }
      catch(const std::invalid_argument&)
      {
        response = invalid_argument_message;
      }
    }
  }
  write(client_socket, response.c_str(), strlen(response.c_str()));
  return response;
}
/*
If the POP3 server issues a positive response, then the response given is multi-line.  After the initial +OK, the
POP3 server sends the message corresponding to the given message-number, being careful to byte-stuff the termination
character (as with all multi-line responses).
*/
string process_retr_command(int &client_socket, char* net_buffer, int full_command_length, float &sequence_phase, vector<EMail> &emails, string &mailbox)
{
  string response;
  // may only be given in the TRANSACTION state
  if(sequence_phase != 1)
    response = bad_sequence_message;
  else
  {
    // a message-number (required) which may NOT refer to a message marked as deleted
    string msg(net_buffer + command_length, full_command_length - command_length);
    msg = msg.substr(0, msg.length() - 2); // Remove \r\n
    try
    {
      int msg_index = stoi(msg);
      if(msg_index < 1 or msg_index > emails.size())
        response = invalid_message;
      else
      {
        if(!emails[msg_index - 1].is_deleted)
        {
          response = "+OK " + to_string(emails[msg_index - 1].size) + " octets\r\n";
          ifstream mailstream;
          mailstream.open(mailbox_directory + "/" + mailbox);
          mailstream.seekg(emails[msg_index - 1].message_start, ios::beg);
          string email = "", line, from_pattern = "From ";
          bool first_message = true;
          // File read: line ends with \n, but for socket writing we'll have to change this to \r\n
          while(getline(mailstream, line))
          {
            if(line.substr(0, from_pattern.length()) == from_pattern)
              break;
            else
            {
              line = line.substr(0, line.size() - 1) + "\r\n";
              email += line;
            }
          }
          response += email;
          response += ".\r\n";
        }
        else
          response = deleted_message;
      }    
    }
    catch(const std::invalid_argument&)
    {
      response = invalid_argument_message;
    }
}
  write(client_socket, response.c_str(), strlen(response.c_str()));
  return response;
}
/*
  The POP3 server marks the message as deleted.  Any future reference to the message-number associated with the message
  in a POP3 command generates an error.  The POP3 server does not actually delete the message until the POP3 session
  enters the UPDATE state.
*/
string process_dele_command(int &client_socket, char* net_buffer, int full_command_length, float &sequence_phase, vector<EMail> &emails)
{
  string response;
  // may only be given in the TRANSACTION state
  if(sequence_phase != 1)
    response = bad_sequence_message;
  else
  {
    // a message-number (required) which may NOT refer to a message marked as deleted
    string msg(net_buffer + command_length, full_command_length - command_length);
    msg = msg.substr(0, msg.length() - 2); // Remove \r\n
    try
    {
      int msg_index = stoi(msg);
      if(msg_index < 1 or msg_index > emails.size())
        response = invalid_message;
      else
      {
        if(!emails[msg_index - 1].is_deleted)
        {
          emails[msg_index - 1].is_deleted = true;
          response = "+OK message " + msg + " deleted\r\n";
        }
        else
          response = deleted_message;
      }    
    }
    catch(const std::invalid_argument&)
    {
      response = invalid_argument_message;
    }
  }
  write(client_socket, response.c_str(), strlen(response.c_str()));
  return response;
}
/*
  If an argument was given and the POP3 server issues a positive response with a line containing information for
  that message.  This line is called a "scan listing" for that message.

  If no argument was given and the POP3 server issues a positive response, then the response given is multi-line.
  After the initial +OK, for each message in the maildrop, the POP3 server responds with a line containing
  information for that message.  This line is also called a "scan listing" for that message.  If there are no
  messages in the maildrop, then the POP3 server responds with no scan listings--it issues a positive response
  followed by a line containing a termination octet and a CRLF pair.

  In order to simplify parsing, all POP3 servers are required to use a certain format for scan listings.  A
  scan listing consists of the message-number of the message, followed by a single space and the exact size of
  the message in octets.  Methods for calculating the exact size of the message are described in the "Message Format"
  section below.  This memo makes no requirement on what follows the message size in the scan listing.  Minimal
  implementations should just end that line of the response with a CRLF pair.  More advanced implementations may
  include other information, as parsed from the message.
*/
string process_list_command(int &client_socket, char* net_buffer, int full_command_length, float &sequence_phase, vector<EMail> &emails)
{
  string response;
  // may only be given in the TRANSACTION state
  if(sequence_phase != 1)
    response = bad_sequence_message;
  else
  {
    // a message-number (optional), which, if present, may NOT refer to a message marked as deleted
    string msg(net_buffer + command_length, full_command_length - command_length);
    if(msg == "\n") // No arguments
    {
      int message_count = 0, maildrop_size = 0;
      for(int i = 0; i< emails.size(); i++)
      {
        if(!emails[i].is_deleted)
        {
          message_count++;
          maildrop_size += emails[i].size;
          response += to_string(i + 1) + " " + to_string(emails[i].size) + "\r\n";
        }
      }
      response = "+OK " + to_string(message_count) + " messages (" + to_string(maildrop_size) + " octets)\r\n" + response + ".\r\n";
    }
    else
    {
      msg = msg.substr(0, msg.length() - 2); // Remove \r\n
      try
      {
        int msg_index = stoi(msg);
        if(msg_index < 1 or msg_index > emails.size())
          response = invalid_message;
        else
        {
          if(!emails[msg_index - 1].is_deleted)
            response = "+OK " + msg + " " + to_string(emails[msg_index - 1].size) + "\r\n";
          else
            response = deleted_message;
        }
      }
      catch(const std::invalid_argument&)
      {
        response = invalid_argument_message;
      }
    }
  }
  write(client_socket, response.c_str(), strlen(response.c_str()));
  return response;
}
/*
 If any messages have been marked as deleted by the POP3 server, they are unmarked.  The POP3 server then replies
 with a positive response.
*/
string process_rset_command(int &client_socket, float &sequence_phase, vector<EMail> &emails)
{
  string response;
  // may only be given in the TRANSACTION state
  if(sequence_phase != 1)
    response = bad_sequence_message;
  else
  {
    int message_count = 0, maildrop_size = 0;
    for(auto &email : emails)
    {
      if(email.is_deleted)
        email.is_deleted = false;
      message_count++;
      maildrop_size += email.size;
    }
    response = "+OK maildrop has " + to_string(message_count) + " messages (" + to_string(maildrop_size) + " octets)\r\n";
  }
  write(client_socket, response.c_str(), strlen(response.c_str()));
  return response;
}
// The POP3 server does nothing, it merely replies with a positive response
string process_noop_command(int &client_socket, float &sequence_phase, vector<EMail> &emails)
{
  string response;
  // may only be given in the TRANSACTION state
  if(sequence_phase != 1)
    response = bad_sequence_message;
  else
    response = ok_message;
  write(client_socket, response.c_str(), strlen(response.c_str()));
  return response;
}
/*
The POP3 server removes all messages marked as deleted from the maildrop and replies as to the status of this
operation.  If there is an error, such as a resource shortage, encountered while removing messages, the
maildrop may result in having some or none of the messages marked as deleted be removed.  In no case may the server
remove any messages not marked as deleted.

Whether the removal was successful or not, the server then releases any exclusive-access lock on the maildrop
and closes the TCP connection.
*/
string process_quit_command(int &client_socket, float &sequence_phase, string &username, vector<EMail> &emails, FILE* &fout)
{
  string response;
  // may only be given in the TRANSACTION state
  if(sequence_phase == 1)
  {
    ofstream outstream;
    outstream.open(mailbox_directory + "/temp.mbox");
    ifstream mailstream;
    mailstream.open(mailbox_directory + "/" + username);
    for(int i = 0; i < emails.size(); i++)
    {
       if(!emails[i].is_deleted)
       {
          mailstream.seekg(emails[i].header_start, ios::beg);   
          string header; 
          getline(mailstream, header);
          header = header.substr(0, header.size() - 1) + "\r\n";
          mailstream.seekg(emails[i].message_start, ios::beg);
          string email = "", line, from_pattern = "From ";
          bool first_message = true;
          // File read: line ends with \n, but for socket writing we'll have to change this to \r\n
          while(getline(mailstream, line))
          {
            if(line.substr(0, from_pattern.length()) == from_pattern)
              break;
            else
            {
              line = line.substr(0, line.size() - 1) + "\r\n";
              email += line;
            }
          }
          outstream<<header<<email;
       }
    }
    outstream.close();
    mailstream.close();
  remove((mailbox_directory + "/" + username).c_str());
  rename((mailbox_directory + "/temp.mbox").c_str(), (mailbox_directory + "/" + username).c_str());
  }  
  response = shutdown_message;
  sleep_for(1s);
  if(fout)
    fclose(fout);
  fout = NULL;
  pthread_mutex_unlock(&mailboxes[username]);
  write(client_socket, response.c_str(), strlen(response.c_str()));
  return response;
}
void *process_client_thread(void *arg)
{	
	/*
	When a client connects, the server should send a simple greeting message.The greeting message should have the form +OK Server ready 
  (Author: Linh Thi Xuan Phan / linhphan), except that you should fill in your own name and SEAS login.
	*/
	int client_socket = *(int*) arg;
  // Once the TCP connection has been opened by a POP3 client, the POP3 server issues a one line greeting.  This can be any positive
  // response.  An example might be: S:  +OK POP3 server ready
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
	bool closeConnection = false;
	float sequence_phase = 0; // Authorization State
  string username;
  vector<EMail> emails;
  FILE* fout;

  /*
  A POP3 session progresses through a number of states during its lifetime.  Once the TCP connection has been opened and the POP3
  server has sent the greeting, the session enters the AUTHORIZATION state.  In this state, the client must identify itself to the POP3
  server.  Once the client has successfully done this, the server acquires resources associated with the client's maildrop, and the
  session enters the TRANSACTION state.  In this state, the client requests actions on the part of the POP3 server.  When the client has
  issued the QUIT command, the session enters the UPDATE state.  In this state, the POP3 server releases any resources acquired during
  the TRANSACTION state and says goodbye.  The TCP connection is then closed.

	-1: Transaction hasn't begun
	0: Authorization State (right after greeting, or after unsuccessful USER/PASS)
  0.5: 0.5 means successful USER
	1: Transaction State (after successful PASS)
	2: Update State
	*/
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
      if(!username.empty())
        pthread_mutex_unlock(&mailboxes[username]);
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
			int full_command_length = command_end_index + suffix_length - net_buffer;
			char* command = new char[command_length]; 
			strncpy(command, net_buffer, command_length);
			command[command_length] = '\0'; 
			string response;
			//  Commands should be treated as case-insensitive - strcasecmp
			if(strcasecmp(command, "USER ") == 0)
				response = process_user_command(client_socket, net_buffer, full_command_length, username, sequence_phase);
			else if(strcasecmp(command, "PASS ") == 0)
				response = process_pass_command(client_socket, net_buffer, full_command_length, username, sequence_phase, emails, fout);
			else if((strcasecmp(command, "STAT\r") == 0)  && (strncmp(net_buffer + command_length, "\n", strlen("\n")) == 0))
				response = process_stat_command(client_socket, sequence_phase, emails);
			else if((strcasecmp(command, "UIDL ") == 0)  || 
				((strcasecmp(command, "UIDL\r") == 0)  && (strncmp(net_buffer + command_length, "\n", strlen("\n")) == 0)))
				response = process_uidl_command(client_socket, net_buffer, full_command_length, sequence_phase, emails);
			else if(strcasecmp(command, "RETR ") == 0)
				response = process_retr_command(client_socket, net_buffer, full_command_length, sequence_phase, emails, username);	  
			else if(strcasecmp(command, "DELE ") == 0)
				response = process_dele_command(client_socket, net_buffer, full_command_length, sequence_phase, emails);	 
			else if((strcasecmp(command, "LIST ") == 0)  || 
				((strcasecmp(command, "LIST\r") == 0)  && (strncmp(net_buffer + command_length, "\n", strlen("\n")) == 0)))
				response = process_list_command(client_socket, net_buffer, full_command_length, sequence_phase, emails);
			else if((strcasecmp(command, "RSET\r") == 0)  && (strncmp(net_buffer + command_length, "\n", strlen("\n")) == 0))
				response = process_rset_command(client_socket, sequence_phase, emails);	     
			else if((strcasecmp(command, "NOOP\r") == 0)  && (strncmp(net_buffer + command_length, "\n", strlen("\n")) == 0))
				response = process_noop_command(client_socket, sequence_phase, emails);	     
			else if((strcasecmp(command, "QUIT\r") == 0)  && (strncmp(net_buffer + command_length, "\n", strlen("\n")) == 0))
      {
				response = process_quit_command(client_socket, sequence_phase, username, emails, fout);	    
        closeConnection = true;
      }  
			else
			{
						response = unknown_message;
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
			delete command;
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
    int option, port = 11000;
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