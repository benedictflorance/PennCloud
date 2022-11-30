#include "utils/globalvars.h"
#include "utils/hash.h"
#include "utils/config_processor.h"
#include "request.pb.h"
#include "response.pb.h"

void clear_keys(PennCloud::Request &request, PennCloud::Response &response, string &request_str, char* response_buffer)
{
    cout<<"Response has a status of "<<response.status()<<" description of "<<response.description()<<" a value of "<<response.value()<<endl;
    request.clear_type();
    request.clear_rowkey();
    request.clear_columnkey();
    request.clear_value1();
    request.clear_value2();
    response.clear_status();
    response.clear_description();
    request_str.clear();
    memset(response_buffer, 0, sizeof(response_buffer));
}
void send_requests(int &sockfd)
{
    string request_str;
    string response_str;
    char response_buffer[BUFFER_SIZE];
    PennCloud::Request request;
    PennCloud::Response response;
    //Test invalid rowkey
    request.set_type("GET");
    request.set_rowkey("benedict");
    request.set_columnkey("password");
    request.SerializeToString(&request_str);
    request_str += "\r\n";
    write(sockfd, request_str.c_str(), strlen(request_str.c_str()));
    while(read(sockfd, response_buffer, BUFFER_SIZE) == 0);
    response.ParseFromString(response_buffer);
    clear_keys(request, response, request_str, response_buffer);

    // // Test valid rowkey
    request.set_type("GET");
    request.set_rowkey("0benedict");
    request.set_columnkey("password");
    request.SerializeToString(&request_str);
    request_str += "\r\n";
    write(sockfd, request_str.c_str(), strlen(request_str.c_str()));
    while(read(sockfd, response_buffer, BUFFER_SIZE) == 0);
    response.ParseFromString(response_buffer);
    clear_keys(request, response, request_str, response_buffer);

    // Test PUT
    request.set_type("PUT");
    request.set_rowkey("0benedict");
    request.set_columnkey("password");
    request.set_value1("ML>>>Systems");
    request.SerializeToString(&request_str);
    request_str += "\r\n";
    write(sockfd, request_str.c_str(), strlen(request_str.c_str()));
    while(read(sockfd, response_buffer, BUFFER_SIZE) == 0);
    response.ParseFromString(response_buffer);
    clear_keys(request, response, request_str, response_buffer);

    // Test valid rowkey
    request.set_type("GET");
    request.set_rowkey("0benedict");
    request.set_columnkey("passwords");
    request.SerializeToString(&request_str);
    request_str += "\r\n";
    write(sockfd, request_str.c_str(), strlen(request_str.c_str()));
    while(read(sockfd, response_buffer, BUFFER_SIZE) == 0);
    response.ParseFromString(response_buffer);
    clear_keys(request, response, request_str, response_buffer);

    // Test invalid rowkey
    request.set_type("GET");
    request.set_rowkey("0benedict");
    request.set_columnkey("password");
    request.SerializeToString(&request_str);
    request_str += "\r\n";
    write(sockfd, request_str.c_str(), strlen(request_str.c_str()));
    while(read(sockfd, response_buffer, BUFFER_SIZE) == 0);
    response.ParseFromString(response_buffer);
    clear_keys(request, response, request_str, response_buffer);

    // Test CPUT invalid condition
    request.set_type("CPUT");
    request.set_rowkey("0benedict");
    request.set_columnkey("password");
    request.set_value1("ML>>>>Systems");
    request.set_value2("SelfDrivingIsGOAT");
    request.SerializeToString(&request_str);
    request_str += "\r\n";
    write(sockfd, request_str.c_str(), strlen(request_str.c_str()));
    while(read(sockfd, response_buffer, BUFFER_SIZE) == 0);
    response.ParseFromString(response_buffer);
    clear_keys(request, response, request_str, response_buffer);

    // Test valid rowkey
    request.set_type("GET");
    request.set_rowkey("0benedict");
    request.set_columnkey("password");
    request.SerializeToString(&request_str);
    request_str += "\r\n";
    write(sockfd, request_str.c_str(), strlen(request_str.c_str()));
    while(read(sockfd, response_buffer, BUFFER_SIZE) == 0);
    response.ParseFromString(response_buffer);
    clear_keys(request, response, request_str, response_buffer);

    // Test CPUT valid condition
    request.set_type("CPUT");
    request.set_rowkey("0benedict");
    request.set_columnkey("password");
    request.set_value1("ML>>>Systems");
    request.set_value2("SelfDrivingIsGOAT");
    request.SerializeToString(&request_str);
    request_str += "\r\n";
    write(sockfd, request_str.c_str(), strlen(request_str.c_str()));
    while(read(sockfd, response_buffer, BUFFER_SIZE) == 0);
    response.ParseFromString(response_buffer);
    clear_keys(request, response, request_str, response_buffer);

    // Test valid rowkey
    request.set_type("GET");
    request.set_rowkey("0benedict");
    request.set_columnkey("password");
    request.SerializeToString(&request_str);
    request_str += "\r\n";
    write(sockfd, request_str.c_str(), strlen(request_str.c_str()));
    while(read(sockfd, response_buffer, BUFFER_SIZE) == 0);
    response.ParseFromString(response_buffer);
    clear_keys(request, response, request_str, response_buffer);

    // Test PUT
    request.set_type("PUT");
    request.set_rowkey("25benny"); 
    request.set_columnkey("cookie");
    request.set_value1("ML>>>Systems");
    request.SerializeToString(&request_str);
    request_str += "\r\n";
    write(sockfd, request_str.c_str(), strlen(request_str.c_str()));
    while(read(sockfd, response_buffer, BUFFER_SIZE) == 0);
    response.ParseFromString(response_buffer);
    clear_keys(request, response, request_str, response_buffer);

    // Test delete 
    request.set_type("DELETE");
    request.set_rowkey("25benny");
    request.set_columnkey("cookie");
    request.SerializeToString(&request_str);
    request_str += "\r\n";
    write(sockfd, request_str.c_str(), strlen(request_str.c_str()));
    while(read(sockfd, response_buffer, BUFFER_SIZE) == 0);
    response.ParseFromString(response_buffer);
    clear_keys(request, response, request_str, response_buffer);

    // Test valid rowkey
    request.set_type("GET");
    request.set_rowkey("25benny");
    request.set_columnkey("cookie");
    request.SerializeToString(&request_str);
    request_str += "\r\n";
    write(sockfd, request_str.c_str(), strlen(request_str.c_str()));
    while(read(sockfd, response_buffer, BUFFER_SIZE) == 0);
    response.ParseFromString(response_buffer);
    clear_keys(request, response, request_str, response_buffer);
}
int create_client()
{
    int sockfd = socket(PF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) 
    {
        if(verbose)
            cerr<<"Unable to create socket"<<endl;
        return -1;

    }
    connect(sockfd, (struct sockaddr*)& tablet_addresses[curr_server_index], sizeof(tablet_addresses[curr_server_index]));
    send_requests(sockfd);
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
    int isSuccess = create_client();
	return isSuccess;
}