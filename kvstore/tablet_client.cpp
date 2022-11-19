#include "globalvars.h"
#include "utils.h"
#include "request.pb.h"

string prepare_request()
{
    string request_str;
    PennCloud::Request request;
    request.set_type("PUT");
    request.set_rowkey("benedict");
    request.set_columnkey("password");
    request.set_value1("ML>>>Systems");
    request.SerializeToString(&request_str);
    return request_str;
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
    string request_str = prepare_request();
    request_str += "\r\n";
    write(sockfd, request_str.c_str(), strlen(request_str.c_str()));
    while(true)
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