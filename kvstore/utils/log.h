void update_log(string file_name, string request);
void replay_log(string file_name);
void clear_log(string file_name);

void update_log(string file_name, string request){
    ofstream log_file;
    log_file.open(file_name, ios::app);
    log_file << request;
    log_file.close();
}

void replay_log(string file_name){
    ifstream log_fstream(file_name);
    string request_str;
    while(getline(log_fstream, request_str)){
        getline(log_fstream, request_str);
        PennCloud::Request request;
        PennCloud::Response response;
	    request.ParseFromString(request_str);
        if (strcasecmp(request.type().c_str(), "PUT") == 0)
			process_put_request(request, response);
		else if (strcasecmp(request.type().c_str(), "CPUT") == 0)
			process_cput_request(request, response);
		else if (strcasecmp(request.type().c_str(), "DELETE") == 0)
			process_delete_request(request, response);
    }
    if(verbose)
        cerr<<"Replayed log file"<<endl;
}

void clear_log(string file_name){
    clear_file(file_name);
}