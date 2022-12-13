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
#include <string>
#include <algorithm>
#include <sys/file.h>
#include <chrono>
#include <ctime> 
#include <thread>
#include <resolv.h>
#include <netdb.h>
#include <map>
#define N 4096
using namespace std;
using namespace std::this_thread;     
using namespace std::chrono_literals; 
const int BUFFER_SIZE = 10000;

class EMail
{
  public:
    int message_start;
    bool is_sent;
    EMail(int message_start, bool is_sent) : message_start(message_start), is_sent(is_sent) {}
};
/*
			Global Variables
*/
string mailbox_directory;
string mailbox = "mqueue.txt";
pthread_mutex_t mutex;
bool verbose = false;
FILE* fout;
vector<EMail> emails;
bool mailbox_loaded = false;

void load_user_mailbox()
{
    ifstream mailstream;
    mailstream.open(mailbox_directory + "/" + mailbox);
    string email, line, begin_pattern = "<BEGINMAIL>";
    int email_start = 0;
    bool first_message = true;
    // File read: line ends with \n, but for socket writing we'll have to change this to \r\n
    while(getline(mailstream, line))
    {   
        if(line.substr(0, line.length()-1) == begin_pattern)
        {
            if(first_message)
                first_message = false;
            else
                emails.push_back(EMail(email_start, false));
            email_start = mailstream.tellg();
        }
    }
    mailstream.close();
    if(!first_message)
        emails.push_back(EMail(email_start, false));
}
void get_ip_addresses(string domain_name, map<int, string> &priority2ip)
{
    u_char nsbuf[N];
    char dispbuf[N];
    ns_msg msg;
    ns_rr rr;
    int i, l;
    l = res_query(domain_name.c_str(), ns_c_in, ns_t_mx, nsbuf, sizeof(nsbuf));
    if (l >= 0)
    {
      ns_initparse(nsbuf, l, &msg);
      l = ns_msg_count(msg, ns_s_an);
      for (i = 0; i < l; i++)
      {
            ns_parserr(&msg, ns_s_an, i, &rr);
            ns_sprintrr(&msg, &rr, NULL, NULL, dispbuf, sizeof(dispbuf));
            string result = string(dispbuf);
            int start = result.find("MX\t");
            string priority_dns = result.substr(start + 3, result.length() - start - 3);
            int space_pos = priority_dns.find(" ");
            int priority = stoi(priority_dns.substr(0, space_pos));
            string host = priority_dns.substr(space_pos + 1, priority_dns.length() - space_pos -1);
            struct hostent *host_entry;
            host_entry = gethostbyname(host.c_str());
            if(host_entry == NULL)
            {   
                if(verbose)
                    cerr<<"No mail server"<<endl;
                continue;
            }
            char* IPbuffer = inet_ntoa(*((struct in_addr*) host_entry->h_addr_list[0]));    
            string ip_address = string(IPbuffer);
            priority2ip[priority] = ip_address;
        }
        if(verbose)
        {
            cerr<<"IP addresses ready! We have "<<priority2ip.size()<<" IP addresses for this domain!"<<endl;
        }
    }
    else
    {
        if(verbose)
            cerr<<"Invalid domain name"<<endl;
    }
}
// This function could be shortened using a helper function to send command and read response, 
// instead of repeating code but not altering it due to lack of time.
bool send_email(string sender, string receiver, string email)
{   
    if(receiver.find("@") == string::npos)
        return false;
    string receiver_domain_name = receiver.substr(receiver.find("@") + 1, receiver.length() - (receiver.find("@") + 1));
    if(sender.find("@") == string::npos)
        return false;
    string sender_domain_name = sender.substr(sender.find("@") + 1, sender.length() - (sender.find("@") + 1));
    map<int, string> ip_addresses;
    get_ip_addresses(receiver_domain_name, ip_addresses);
    if(ip_addresses.size() < 1)
        return false;
    bool sentEmail = false;
    for(auto ip_address : ip_addresses)
    {
        int sockfd = socket(PF_INET, SOCK_STREAM, 0);
        if (sockfd < 0) 
        {
            if(verbose)
                cerr<<"Unable to create socket"<<endl;
            continue;

        }
        struct sockaddr_in servaddr;
        bzero(&servaddr, sizeof(servaddr));
        servaddr.sin_family = AF_INET;
        servaddr.sin_port = htons(25);
        inet_pton(AF_INET, (ip_address.second).c_str(), &(servaddr.sin_addr));
        connect(sockfd, (struct sockaddr*)&servaddr, sizeof(servaddr));
        /* 
        Greeting phase
        */
        char *response_buffer = new char[BUFFER_SIZE];
        int read_return = 0;
        string command_return_val, response;
        memset(response_buffer, 0, sizeof(response_buffer)); 
        sleep_for(1s);       
        while((read_return = read(sockfd, response_buffer, BUFFER_SIZE)) == 0);
        response = string(response_buffer);
        if(response.length() < 3 || response.substr(0, 3) != "220")
        {
            if(verbose)
                cerr<<"Greeting failure"<<endl;
            close(sockfd);
            continue;
        }
        else
        {
            if(verbose)
                cerr<<"Greeting success"<<endl;
        }
        /*
        HELO phase
        */
        memset(response_buffer, 0, sizeof(response_buffer));  
        sleep_for(1s);
        response.clear();
        string command = "HELO " + sender_domain_name + "\r\n";
        write(sockfd, command.c_str(), strlen(command.c_str()));
        read(sockfd, response_buffer, BUFFER_SIZE);
        response = string(response_buffer);
        if(response.length() < 3 || response.substr(0, 3) != "250")
        {
            if(verbose)
                cerr<<command.substr(0, command.size() - 2) + " failure"<<endl;
            close(sockfd);
            continue;
        }
        else
        {
            if(verbose)
                cerr<<command.substr(0, command.size() - 2) + " success"<<endl;
        }
        /*
        MAIL FROM phase
        */
        memset(response_buffer, 0, sizeof(response_buffer));  
        sleep_for(1s);
        response.clear();
        command = "MAIL FROM:<" + sender +">\r\n";
        write(sockfd, command.c_str(), strlen(command.c_str()));
        read(sockfd, response_buffer, BUFFER_SIZE);
        response = string(response_buffer);
        if(response.length() < 3 || response.substr(0, 3) != "250")
        {
            if(verbose)
                cerr<<command.substr(0, command.size() - 2) + " failure"<<endl;
            close(sockfd);
            continue;
        }
        else
        {
            if(verbose)
                cerr<<command.substr(0, command.size() - 2) + " success"<<endl;
        }
        /*
        RCPT TO phase
        */
        memset(response_buffer, 0, sizeof(response_buffer));  
        sleep_for(1s);
        response.clear();
        command = "RCPT TO:<" + receiver +">\r\n";
        write(sockfd, command.c_str(), strlen(command.c_str()));
        read(sockfd, response_buffer, BUFFER_SIZE);
        response = string(response_buffer);
        if(response.length() < 3 || response.substr(0, 3) != "250")
        {
            if(verbose)
                cerr<<command.substr(0, command.size() - 2) + " failure"<<endl;
            close(sockfd);
            continue;
        }
        else
        {
            if(verbose)
                cerr<<command.substr(0, command.size() - 2) + " success"<<endl;
        }
        /*
        DATA phase
        */
        memset(response_buffer, 0, sizeof(response_buffer));  
        sleep_for(1s);
        response.clear();
        command = "DATA\r\n";
        write(sockfd, command.c_str(), strlen(command.c_str()));
        read(sockfd, response_buffer, BUFFER_SIZE);
        response = string(response_buffer);
        if(response.length() < 3 || (response.substr(0, 3) != "354"))
        {
            if(verbose)
                cerr<<command.substr(0, command.size() - 2) + " failure"<<endl;
            close(sockfd);
            continue;
        }
        else
        {
            if(verbose)
                cerr<<command.substr(0, command.size() - 2) + " success"<<endl;
        }
        /*
        DATA writing phase
        */
        write(sockfd, email.c_str(), strlen(email.c_str()));
        /*
        DATA END phase
        */
        memset(response_buffer, 0, sizeof(response_buffer));  
        sleep_for(1s);
        response.clear();
        command = ".\r\n";
        write(sockfd, command.c_str(), strlen(command.c_str()));
        read(sockfd, response_buffer, BUFFER_SIZE);
        response = string(response_buffer);
        if(response.length() < 3 || response.substr(0, 3) != "250")
        {
            if(verbose)
                cerr<<"Data writing failure"<<endl;
            close(sockfd);
            continue;
        }
        else
        {
            if(verbose)
            {
                cerr<<"Data writing success"<<endl;
            }
            sentEmail = true;
            break;
        }
        /*
        QUIT phase
        */
        memset(response_buffer, 0, sizeof(response_buffer));  
        sleep_for(1s);
        response.clear();
        command = "QUIT\r\n";
        write(sockfd, command.c_str(), strlen(command.c_str()));
        read(sockfd, response_buffer, BUFFER_SIZE);
        response = string(response_buffer);
        if(response.length() < 3 || response.substr(0, 3) != "221")
        {
            if(verbose)
                cerr<<command.substr(0, command.size() - 2) + " failure"<<endl;
        }
        else
        {
            if(verbose)
                cerr<<command.substr(0, command.size() - 2) + " success"<<endl;
        }
        close(sockfd);
    }
    return sentEmail;
}
void send_emails()
{
    ifstream mailstream;
    mailstream.open(mailbox_directory + "/" + mailbox);
    int sent_emails = 0;
    for(int i = 0; i < emails.size(); i++)
    {
        mailstream.seekg(emails[i].message_start, ios::beg);
        string email = "", line, begin_pattern = "<BEGINMAIL>";
        string sender, receiver;
        getline(mailstream, sender);
        sender = sender.substr(0, sender.size() - 1);
        getline(mailstream, receiver);
        receiver = receiver.substr(0, receiver.size() - 1);
        if(verbose)
        {
            cerr<<"Sender: "<<sender<<endl;
            cerr<<"Receiver: "<<receiver<<endl;
        }
        bool first_message = true;
        // File read: line ends with \n, but for socket writing we'll have to change this to \r\n
        while(getline(mailstream, line))
        {
            if(line.substr(0, line.length()-1) == begin_pattern)
                break;
            else
            {
                line = line.substr(0, line.size() - 1) + "\r\n";
                email += line;
            }
        }
        bool isSent = send_email(sender, receiver, email);
        if(verbose)
        {
            cerr<<"Email Sent? "<<isSent<<endl;
        }
        if(isSent)
        {
            emails[i].is_sent = true;
            sent_emails++;
        }
    }
    mailstream.close();
    if(verbose)
    {
        cerr<<"Total emails in mqueue: "<<emails.size()<<endl;
        cerr<<"Delivered emails: "<<sent_emails<<endl;
        cerr<<"Undelivered emails: "<<emails.size() - sent_emails<<endl;
    }
}
void start_quit_routine()
{
    ofstream outstream;
    outstream.open(mailbox_directory + "/temp.txt");
    ifstream mailstream;
    mailstream.open(mailbox_directory + "/" + mailbox);
    for(int i = 0; i < emails.size(); i++)
    {
        if(!emails[i].is_sent)
        {
            mailstream.seekg(emails[i].message_start, ios::beg);
            string email = "", line, begin_pattern = "<BEGINMAIL>";
            bool first_message = true;
            // File read: line ends with \n, but for socket writing we'll have to change this to \r\n
            while(getline(mailstream, line))
            {
                if(line.substr(0, line.length()-1) == begin_pattern)
                    break;
                else
                {
                    line = line.substr(0, line.size() - 1) + "\r\n";
                    email += line;
                }
            }
            outstream<<begin_pattern + "\r\n"<<email;
        }
    }
    outstream.close();
    mailstream.close();
    remove((mailbox_directory + "/" + mailbox).c_str());
    rename((mailbox_directory + "/temp.txt").c_str(), (mailbox_directory + "/" + mailbox).c_str());
    if(verbose)
        cerr<<"Shutting down!";
    sleep_for(1s);
    if(fout)
        fclose(fout);
    fout = NULL;
    pthread_mutex_unlock(&mutex);
    exit(0);
}
void signal_handler(int arg)
{
    if(mailbox_loaded)
    {
        if(verbose)
            cerr<<"Interrupt received. Mailbox loaded, so executing quit routine!"<<endl;
        start_quit_routine();
    }
}
int main(int argc, char *argv[])
{
    /* 
    The server should support three command-line options: -p <portno>, -a, and -v. If the -p option is
    given, the server should accept connections on the specified port; otherwise it should use port 10000. 
    If the -a option is given, the server should output your full name and SEAS login to stderr, and then 
    exit. If the -v option is given, the server should print debug output to stderr.  
    */
	signal(SIGINT, signal_handler);
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
    pthread_mutex_lock(&mutex);
    fout = fopen((mailbox_directory + "/" + mailbox).c_str(), "rw");
    int flock_return = flock(fileno(fout), LOCK_EX | LOCK_NB);
    if(flock_return == 0)
    {
        if(verbose)
            cerr<<"Acquired lock!"<<endl;
        load_user_mailbox();
        mailbox_loaded = true;
        send_emails();
        start_quit_routine();
    }
    else
    {
        if(verbose)
            cerr<<"Unable to acquire lock to mqueue"<<endl;
        return -1;
    }
    return 0;
}