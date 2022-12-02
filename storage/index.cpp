#include "../http/http.hpp"
#include "../kvstore/client_wrapper.h"
#include <unistd.h>

#include <iostream>
#include <sstream>
#include <thread>

#include <ext/stdio_filebuf.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <vector>
#include <string.h>
KVstore store;
std::string user = "testuser";
const std::string files = "_files"; 
std::vector<std::string> filelist;


std::string urlEncode(std::string str){
    std::string new_str = "";
    char c;
    int ic;
    const char* chars = str.c_str();
    char bufHex[10];
    int len = str.length();
    for(int i=0;i<len;i++){
        c = chars[i];
        ic = c;
    	if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') new_str += c;
        else {
            sprintf(bufHex,"%X",c);
            if(ic < 16) 
                new_str += "%0"; 
            else
                new_str += "%";
            new_str += bufHex;
        }
    }
    return new_str;
 }

std::string urlDecode(std::string str){
    std::string ret;
    char ch;
    int i, ii, len = str.length();

    for (i=0; i < len; i++){
        if(str[i] != '%'){
            if(str[i] == '+')
                ret += ' ';
            else
                ret += str[i];
        }else{
            sscanf(str.substr(i + 1, 2).c_str(), "%x", &ii);
            ch = static_cast<char>(ii);
            ret += ch;
            i = i + 2;
        }
    }
    return ret;
}



std::string serial_vector(std::vector<std::string> vec) {
	std::string s = "";
	for(int i = 0; i < vec.size(); i++) {
		s += vec[i];
		s += "\r\n";
	}
	return s;
}

std::vector<std::string> deserial_vector(std::string str) {
std::vector<std::string> ret;
	std::size_t pos  = 0;
	while(pos < str.size()) {
		const std::size_t eq = str.find("\r\n", pos);
		std::string key = str.substr(pos, eq - pos);
		ret.push_back(key);
		pos = eq + 2;

	}
	return ret;
}


static std::unique_ptr<std::istream> index_page(http::Response &resp) {
	http::Session &session = resp.get_session();
	session.set_username(user);
	if (session.get_username().empty()) {
		resp.resp_headers.emplace("Content-Type", "text/html");
		return std::make_unique<std::ifstream>("static/login.html");
	}

	resp.resp_headers.emplace("Content-Type", "text/html");
	std::unique_ptr<std::stringstream> ss = std::make_unique<std::stringstream>();
	std::ifstream file( "static/upload.html" );
	for(auto it = filelist.begin(); it != filelist.end(); it++) {
		char put[1000];
		std::string encoded = urlEncode(*it);
		sprintf(put, R"(<br /><a href="/download?filename=%s" download=%s>Download %s</a>)", encoded.c_str(), it->c_str(), it->c_str());
		*ss << put << std::endl;
	}
    if ( file )
    {
        *ss << file.rdbuf();
        file.close();
    }

	return ss;
}

static std::unique_ptr<std::istream> test(http::Response &resp) {
	http::Session &session = resp.get_session();
	session.set_username(user);
	if (session.get_username().empty()) {
		resp.resp_headers.emplace("Content-Type", "text/html");
		return std::make_unique<std::ifstream>("static/login.html");
	}

	resp.resp_headers.emplace("Content-Type", "text/html");
	std::unique_ptr<std::stringstream> ss = std::make_unique<std::stringstream>();
	*ss << "Welcome, " << session.get_username() << "!";
	*ss << R"(<br /><a href="/logout">Logout</a>)";
	return ss;
}




static std::unique_ptr<std::istream> post_file(http::Response &resp) {
	const std::unordered_map<std::string, std::string> form = resp.parse_file_upload();
	std::string username, password;
	http::Session &session = resp.get_session();
	session.set_username(user);
	if (session.get_username().empty()) {
		resp.resp_headers.emplace("Content-Type", "text/html");
		return std::make_unique<std::ifstream>("static/login.html");
	}

	const auto it = form.find("filename");
	const auto it2 = form.find("content");
	if(it == form.end()) {
					throw http::Exception(http::Status::BAD_REQUEST, "Bad Request");
	}

	filelist.push_back(it->second);
	kvstore.put(user, files, serial_vector(filelist));
	kvstore.put(user, it->second, it2->second);
	std::unique_ptr<std::stringstream> ss = std::make_unique<std::stringstream>();
	//*ss << "Welcome, " << session.get_username() << "!";
	*ss << R"(<br /><a href="/">Go Back</a>)";
	return ss;
}


static std::unique_ptr<std::istream> get_file(http::Response &resp) {
	const std::unordered_map<std::string, std::string> params = resp.get_params();
	if(params.find("filename") == params.end()) {
			throw http::Exception(http::Status::BAD_REQUEST, "Bad Request");
	}
	std::string filename = urlDecode(params.find("filename")->second);
	std::string file = kvstore.get(user, filename);
	std::unique_ptr<std::stringstream> ss = std::make_unique<std::stringstream>();
	//*ss << "Welcome, " << session.get_username() << "!";
	*ss << file;
	return ss;
}



static std::unique_ptr<std::istream> post_data(http::Response &resp) {
	const std::unordered_map<std::string, std::string> form = resp.parse_www_form();
	for (auto it = form.begin(); it != form.end(); it++) {
		std::cout << it->first << " " << it->second << std::endl;
	}
	return nullptr;

}



static void handle(int client) {
	try {
		http::handle_socket(client);
	} catch (const std::exception &e) {
		std::cerr << "Error: " << e.what() << std::endl;
	}
}

int main() {
	http::register_handler("/", http::Method::GET, index_page);
	http::register_handler("/index", http::Method::GET, index_page);
	//http::register_handler("/dynamic", http::Method::GET, dynamic_content);
	http::register_handler("/upload", http::Method::POST, post_file);
	http::register_handler("/download", http::Method::GET, get_file);
	std::string config_file = "../kvstore/configs/tablet_server_config.txt";
	//process_config_file(config_file);
		const int sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (sock == -1) {
		throw std::system_error(errno, std::generic_category(), "socket");
	}

	const sockaddr_in addr = {
		.sin_family = AF_INET,
		.sin_port = htons(10001),
		.sin_addr = {.s_addr = INADDR_ANY},
	};
	std::string file = kvstore.get(user, file);
	if(file != "0") filelist = deserial_vector(file);
	if (bind(sock, reinterpret_cast<const sockaddr *>(&addr), sizeof(addr)) == -1) {
		throw std::system_error(errno, std::generic_category(), "bind");
	}

	if (listen(sock, 10) == -1) {
		throw std::system_error(errno, std::generic_category(), "listen");
	}
	while (true) {
		const int client = accept(sock, nullptr, nullptr);
		if (client == -1) {
			throw std::system_error(errno, std::generic_category(), "accept");
		}

		std::thread(handle, client).detach();
	}
}