#include "../http/http.hpp"
#include <unistd.h>
#include "../kvstore/client_wrapper.h"
#include <iostream>
#include <sstream>
#include <thread>
#include <ext/stdio_filebuf.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <vector>
#include <string.h>


std::string user = "09090";
const std::string files = "_root"; 
std::vector<std::pair<std::string, std::string>> filelist;


void split(const std::string& str, const std::string& delim, std::vector<std::string>& parts) {
  size_t start, end = 0;
  while (end < str.size()) {
    start = end;
    while (start < str.size() && (delim.find(str[start]) != std::string::npos)) {
      start++;  // skip initial whitespace
    }
    end = start;
    while (end < str.size() && (delim.find(str[end]) == std::string::npos)) {
      end++; // skip to end of word
    }
    if (end-start != 0) {  // just ignore zero-length strings.
      parts.push_back(std::string(str, start, end-start));
    }
  }
}


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



std::string serial_vector(std::vector<std::pair<std::string, std::string>> vec) {
	std::string s = "";
	for(int i = 0; i < vec.size(); i++) {
		s += vec[i].first;
		s += "****";
		s += vec[i].second;
		s += ",,,";
	}
	return s;
}

std::vector<std::pair<std::string, std::string>> deserial_vector(std::string str) {
	std::vector<std::pair<std::string, std::string>> ret;
	std::size_t pos  = 0;
	while(pos < str.size()) {
		 std::size_t eq = str.find("****", pos);
		std::string key = str.substr(pos, eq - pos);
		pos = eq + 4;
		eq = str.find(",,,", pos);
		std::string value = str.substr(pos, eq - pos);
		std::pair<std::string, std::string> put = std::pair<std::string, std::string>(key, value);
		ret.push_back(put);
		pos = eq + 3;
	}
	return ret;
}

std::string nav_filepath(std::string username, std::string filepath, std::vector<std::string> splitted, std::vector<std::pair<std::string, std::string>> &list) {
		split(filepath, "/", splitted);
		std::string search = "_root";
		if(splitted.empty()) {
			std::string fileval = kvstore.get(username, search);
			if(fileval != "") list = deserial_vector(fileval);
			return search;
		}
		for(int i = 0; i < splitted.size(); i++) {
			std::string fileval = kvstore.get(username, search);
			if(fileval != "") list = deserial_vector(fileval);
			int index = -1;
			for(int j = 0; j < list.size(); j++) {
				if(list[j].first == splitted[i]) {
					search = list[j].second;
					index = j;
				}
			}
			if (index < 0) return "";
		}
	return search;

}

static std::unique_ptr<std::istream> return_val(http::Response &resp) {
	std::unique_ptr<std::stringstream> ss = std::make_unique<std::stringstream>();
	const std::unordered_map<std::string, std::string> params = resp.get_params();
	resp.session.set_username(user);
	if (resp.session.get_username().empty()) {
		resp.resp_headers.emplace("Content-Type", "text/html");
		return std::make_unique<std::ifstream>("static/login.html");
	}
	std::string username =resp.session.get_username();
	if(params.find("filepath") == params.end()) {
			throw http::Exception(http::Status::BAD_REQUEST, "Bad Request");
	}
		const auto it = params.find("filepath");
		const auto it2 = params.find("filename");
		std::vector<std::string> splitted;
		std::vector<std::pair<std::string, std::string>> list;
		split(it->second, "/", splitted);
		splitted.push_back(it2->second);
		std::string search = "_root";
		for(int i = 0; i < splitted.size(); i++) {
			std::string fileval = kvstore.get(username, search);
			if(fileval != "") list = deserial_vector(fileval);
			int index = -1;
			for(int j = 0; j < list.size(); j++) {
				if(list[i].first == splitted[i]) {
					search = list[i].second;
					index = i;
				}
			}
			if (index < 0) return ss;
		}
		std::string ret = kvstore.get(username, search);
		*ss << ret << std::endl;
		return ss;


}

static std::unique_ptr<std::istream> create_val(http::Response &resp) {
	std::unique_ptr<std::stringstream> ss = std::make_unique<std::stringstream>();
	const std::unordered_map<std::string, std::string> params = resp.get_params();
	resp.session.set_username(user);
	if (resp.session.get_username().empty()) {
		resp.resp_headers.emplace("Content-Type", "text/html");
		return std::make_unique<std::ifstream>("static/login.html");
	}
	std::string username = resp.session.get_username();
	if(params.find("filepath") == params.end()) {
			throw http::Exception(http::Status::BAD_REQUEST, "Bad Request");
	}
		const auto it = params.find("filepath");
		const auto it2 = params.find("filename");
		std::vector<std::string> splitted;
		std::vector<std::pair<std::string, std::string>> list;
		split(it->second, "/", splitted);
		std::string search = "_root";
		for(int i = 0; i < splitted.size(); i++) {
			std::string fileval = kvstore.get(username, search);
			if(fileval != "") list = deserial_vector(fileval);
			int index = -1;
			for(int j = 0; j < list.size(); j++) {
				if(list[i].first == splitted[i]) {
					search = list[i].second;
					index = i;
				}
			}
			if (index < 0) return ss;
		}
		std::string loc = std::to_string(rand() % 10000);
		list.push_back(std::pair<std::string, std::string>(it2->second, loc));
		kvstore.put(username, search, serial_vector(list));
		*ss << "Success!!" << std::endl;
		return ss;


}

static std::unique_ptr<std::istream> create_folder(http::Response &resp) {
	std::unique_ptr<std::stringstream> ss = std::make_unique<std::stringstream>();
	const std::unordered_map<std::string, std::string> params = resp.get_params();
	resp.session.set_username(user);
	if (resp.session.get_username().empty()) {
		resp.resp_headers.emplace("Content-Type", "text/html");
		return std::make_unique<std::ifstream>("static/login.html");
	}
	const std::unordered_map<std::string, std::string> form = resp.parse_www_form();
	std::string username = resp.session.get_username();
	if(params.find("filepath") == params.end()) {
			throw http::Exception(http::Status::BAD_REQUEST, "Bad Request");
	}
		const auto it = params.find("filepath");
		const auto it2 = form.find("foldername");
		std::string newval = it2->second;
		std::vector<std::string> splitted;
		std::vector<std::pair<std::string, std::string>> list;
		std::string search = nav_filepath(username, it->second, splitted, list);
		std::string loc = std::to_string(-1 * (rand() % 10000));
		list.push_back(std::pair<std::string, std::string>(newval, loc));
		kvstore.put(username, search, serial_vector(list));
		*ss << "Success!!" << std::endl;
		return ss;
}


static std::unique_ptr<std::istream> index_page(http::Response &resp) {
	resp.session.set_username(user);
	std::string username = resp.session.get_username();
	if (username == "") {
		resp.resp_headers.emplace("Content-Type", "text/html");
		return std::make_unique<std::ifstream>("static/login.html");
	}
	const std::unordered_map<std::string, std::string> params = resp.get_params();
	std::string val = "";
	const auto iit = params.find("filepath");
	if (iit != params.end()) val = urlDecode(iit->second);
	std::vector<std::string> splitted;
	std::vector<std::pair<std::string, std::string>> list;
	split(val, "/", splitted);
	std::string search = nav_filepath(username, val, splitted, list);
	resp.resp_headers.emplace("Content-Type", "text/html");
	std::unique_ptr<std::stringstream> ss = std::make_unique<std::stringstream>();
	std::string fileval = kvstore.get(user, search);
	if(fileval != "") filelist = deserial_vector(fileval);
	else filelist = std::vector<std::pair<std::string, std::string>>();
	*ss << "<html><head><title>This is the title of the webpage!</title></head><body>" << std::endl;
	std::string table = "<table>  <tr> <th>Name</th> <th>Download</th><th>Delete</th></tr>";
	for(auto it = filelist.begin(); it != filelist.end(); it++) {
		char put[1000];
		std::string temp = it->first;
		std::string encoded = urlEncode(it->first);
		std::string link = it->second;
		table += "<tr>";
		if(atoi(link.c_str()) >= 0) {
		table += ("<td>" + it->first + "</td>");
		sprintf(put, R"(<td><br /><a href="/download?filename=%s" download=%s>Download %s</a></td>)", encoded.c_str(), temp.c_str(), temp.c_str());
		table += put;
		memset(put, 0, sizeof(put));
		sprintf(put, R"(<td><br /><a href="/delete?filename=%s">Delete File</a></td>)", encoded.c_str());
		table += put;
		table += "</tr>";
		}
		else {
		table += ("<td>" + it->first + "</td>");
		std::string encodepath = urlEncode(val + "/" + encoded);
		sprintf(put, R"(<td><br /><a href="/?filepath=%s">Access %s</a></td>)", encodepath.c_str(), temp.c_str());
		table += put;
		memset(put, 0, sizeof(put));
		sprintf(put, R"(<td><br /><a href="/delete?filename=%s">Delete Folder</a></td>)", encoded.c_str());
		table += put;
		table += "</tr>";
		}
	}
	table += "</table>";
	*ss << table << std::endl;
	char form[1000];
		sprintf(form,  R"(<form action="/upload?filepath=%s" enctype="multipart/form-data" method="post">
            <input type="file" id="myFile"  name="filename">
            <input type="submit">
          </form></body></html>)", val.c_str());
	*ss << form << std::endl;
	memset(form, 0, sizeof(form));
	sprintf(form,  R"(<form action="/upload_folder?filepath=%s" enctype="application/x-www-form-urlencoded" method="post">
            <input type="text" id="myFile"  name="foldername">
            <input type="submit">
          </form></body></html>)", val.c_str());
	*ss << form << std::endl;

	return ss;
}

static std::unique_ptr<std::istream> test(http::Response &resp) {
	resp.session.set_username(user);
	if (resp.session.get_username().empty()) {
		resp.resp_headers.emplace("Content-Type", "text/html");
		return std::make_unique<std::ifstream>("static/login.html");
	}

	resp.resp_headers.emplace("Content-Type", "text/html");
	std::unique_ptr<std::stringstream> ss = std::make_unique<std::stringstream>();
	*ss << "Welcome, " << resp.session.get_username() << "!";
	*ss << R"(<br /><a href="/logout">Logout</a>)";
	return ss;
}

static std::unique_ptr<std::istream> post_file(http::Response &resp) {
	const std::unordered_map<std::string, std::string> form = resp.parse_file_upload();
	std::string username, password;
	const std::unordered_map<std::string, std::string> params = resp.get_params();
	resp.session.set_username(user);
	if (resp.session.get_username().empty()) {
		resp.resp_headers.emplace("Content-Type", "text/html");
		return std::make_unique<std::ifstream>("static/login.html");
	}
	username = resp.session.get_username();
	const auto it = form.find("filename");
	const auto it3 = params.find("filepath");
	const auto it2 = form.find("content");
	if(it == form.end()) {
					throw http::Exception(http::Status::BAD_REQUEST, "Bad Request");
	}
	std::vector<std::string> splitted;
	std::vector<std::pair<std::string, std::string>> list;
	std::string val = it3->second;
	split(val, "/", splitted);
	std::string search = nav_filepath(username, val, splitted, list);
	resp.resp_headers.emplace("Content-Type", "text/html");
	std::unique_ptr<std::stringstream> ss = std::make_unique<std::stringstream>();
	std::string fileval = kvstore.get(user, search);
	if(fileval != "") filelist = deserial_vector(fileval);
	std::string loc = std::to_string( (rand() % 10000));
	filelist.push_back(std::pair<std::string, std::string>(it->second, loc));
	kvstore.put(user, files, serial_vector(filelist));
	kvstore.put(user, loc, it2->second);
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
	std::string key = "";
	for(int i = 0; i < filelist.size(); i++) {
		if(filename == filelist[i].first) key = filelist[i].second;
	}
	std::string file = kvstore.get(user, key);
	std::unique_ptr<std::stringstream> ss = std::make_unique<std::stringstream>();
	//*ss << "Welcome, " << session.get_username() << "!";
	*ss << file;
	return ss;
}

static std::unique_ptr<std::istream> delete_file(http::Response &resp) {
	const std::unordered_map<std::string, std::string> form = resp.parse_file_upload();
	std::string username, password;
	http::Session &session = resp.session;
	const std::unordered_map<std::string, std::string> params = resp.get_params();
	if (session.get_username().empty()) {
		resp.resp_headers.emplace("Content-Type", "text/html");
		return std::make_unique<std::ifstream>("static/login.html");
	}
	const auto it = form.find("filename");
	const auto it3 = params.find("filepath");
	if(it == form.end()) {
					throw http::Exception(http::Status::BAD_REQUEST, "Bad Request");
	}
	std::vector<std::string> splitted;
	std::vector<std::pair<std::string, std::string>> list;
	std::string val = it3->second, filename = it->second;
	split(val, "/", splitted);
	std::string search = nav_filepath(username, val, splitted, list);
	resp.resp_headers.emplace("Content-Type", "text/html");
	std::unique_ptr<std::stringstream> ss = std::make_unique<std::stringstream>();
	std::string fileval = kvstore.get(user, search);
	if(fileval != "") filelist = deserial_vector(fileval);
	//*ss << "Welcome, " << session.get_username() << "!";
	*ss << R"(<br /><a href="/">Go Back</a>)";
	std::string key = "";
	int index = -1;
	for(int i = 0; i < filelist.size(); i++) {
		if(filename == filelist[i].first) {
			key = filelist[i].second;
			index = i;
	}
	}
	filelist.erase(filelist.begin() + index);
	kvstore.dele(username, key);
	*ss << "File deleted";
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
	http::register_handler("/upload_folder", http::Method::POST, create_folder);
	std::string config_file = "../kvstore/configs/tablet_server_config.txt";
	//process_config_file(config_file);
		const int sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (sock == -1) {
		throw std::system_error(errno, std::generic_category(), "socket");
	}

	const sockaddr_in addr = {
		.sin_family = AF_INET,
		.sin_port = htons(10002),
		.sin_addr = {.s_addr = INADDR_ANY},
	};
	std::string file = "";
	file = kvstore.get(user, "_root");
	if(file != "") filelist = deserial_vector(file);
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