#include "../request.pb.h"
#include "../response.pb.h"

void update_log(string file_name,string metadata_file, string request_type, 
string row_key, string col_key, string value1, string value2);
void replay_log(string file_name, string metadata_file);
void clear_log(string file_name, string metadata_file);
void process_request(string req, string rowk, string colk, string v1, string v2);

void update_log(string file_name, string metadata_file, string request_type, string row_key, string col_key, string value1, string value2){
    ofstream meta_file;
    ofstream log_file;
    log_file.open(file_name, ios::app);
    meta_file.open(metadata_file, ios::app);
    int req_start = log_file.tellp();
    log_file << request_type;
    int rkey_start = log_file.tellp();
    log_file << row_key;
    int ckey_start = log_file.tellp();
    log_file << col_key;
    int v1_start = log_file.tellp();
    log_file << value1;
    int v2_start = log_file.tellp();
    log_file << value2;
    meta_file << req_start  << ","<< request_type.length() << "," << rkey_start << "," << row_key.length()  << "," << 
    ckey_start << "," << col_key.length() << "," << v1_start << "," << value1.length()  << "," << v2_start << "," << value2.length()<<"\n";

    log_file.close();
    meta_file.close();
}

void replay_log(string file_name, string metadata_file){
    fstream log_file(file_name, ios::in);
    fstream meta_file(metadata_file, ios::in);

    if(verbose)
        cout<<"Replaying log"<<endl;

    string line;
    char *req_char = new char[RKEY_BUFFER_SIZE];
    char *rkey_char = new char[RKEY_BUFFER_SIZE];
    char *ckey_char = new char[CKEY_BUFFER_SIZE];
    char *value1_char = new char[BUFFER_SIZE];
    char *value2_char = new char[BUFFER_SIZE];
    while(getline(meta_file,line)){
        stringstream meta(line); 
        string req_start_str, req_size_str, rkey_start_str, rkey_size_str, ckey_start_str, ckey_size_str,
        val1_start_str, val1_size_str, val2_start_str, val2_size_str;
        getline(meta, req_start_str, ',');
        getline(meta, req_size_str, ',');
        getline(meta, rkey_start_str, ',');
        getline(meta, rkey_size_str, ',');
        getline(meta, ckey_start_str, ',');
        getline(meta, ckey_size_str, ',');
        getline(meta, val1_start_str, ',');
        getline(meta, val1_size_str, ',');
        getline(meta, val2_start_str, ',');
        getline(meta, val2_size_str, ',');
        int req_start = stoi(req_start_str),
        req_size = stoi(req_size_str),
        rkey_start = stoi(rkey_start_str), 
        rkey_size = stoi(rkey_size_str), 
        ckey_start = stoi(ckey_start_str), 
        ckey_size = stoi(ckey_size_str), 
        val1_start = stoi(val1_start_str), 
        val1_size= stoi(val1_size_str),
        val2_start = stoi(val2_start_str), 
        val2_size= stoi(val2_size_str);
        memset(req_char, 0, sizeof(req_char));
        memset(rkey_char, 0, sizeof(rkey_char));
        memset(ckey_char, 0, sizeof(ckey_char));
        memset(value1_char, 0, sizeof(value1_char));
        memset(value2_char, 0, sizeof(value2_char));
        log_file.seekg(req_start, ios::beg);
        log_file.read(req_char, req_size);
        log_file.seekg(rkey_start, ios::beg);
        log_file.read(rkey_char, rkey_size);
        log_file.seekg(ckey_start, ios::beg);
        log_file.read(ckey_char, ckey_size);
        log_file.seekg(val1_start, ios::beg);
        log_file.read(value1_char, val1_size);
        log_file.seekg(val2_start, ios::beg);
        log_file.read(value2_char, val2_size);
        string request(req_char),
        rowk(rkey_char,rkey_size),
        colk(ckey_char,ckey_size),
        v1(value1_char,val1_size),
        v2(value2_char, val2_size);

        if(verbose)
            cout<<request<<" "<<rowk<<" "<<colk<<" "<<v1.size()<<" "<<v2.size()<<endl;
        
        process_request(request, rowk, colk, v1, v2);
    }
    delete req_char;
    delete rkey_char;
    delete ckey_char;
    delete value1_char;
    delete value2_char;
}

void process_request(string req, string rowk, string colk, string v1, string v2){
    PennCloud::Request request;
    request.set_type(req);
    request.set_rowkey(rowk);
    request.set_columnkey(colk);
    request.set_value1(v1);
    request.set_value2(v2);
	PennCloud::Response response;
    if (strcasecmp(request.type().c_str(), "PUT") == 0){
		process_put_request(request, response);
	}
    else if (strcasecmp(request.type().c_str(), "CPUT") == 0){
		process_cput_request(request, response);
	}
	else if (strcasecmp(request.type().c_str(), "DELETE") == 0){
		process_delete_request(request, response);
	}
}


void clear_log(string file_name, string metadata_file){
    clear_file(file_name);
    clear_file(metadata_file);
}