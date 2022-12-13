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
#include "mail.pb.h"
#include <openssl/md5.h>

std::string user = "09090";
const std::string files = "_files"; 
std::vector<std::string> filelist;
std::vector<PennCloud::Mail> emails;
void computeDigest(char *data, int dataLengthBytes, unsigned char *digestBuffer)
{

  MD5_CTX c;
  MD5_Init(&c);
  MD5_Update(&c, data, dataLengthBytes);
  MD5_Final(digestBuffer, &c);
}


std::string compute_hash(std::string header, std::string email)
{
  unsigned char* digest = new unsigned char[MD5_DIGEST_LENGTH];
  std::string full_email_string = header + email;
  char full_email[full_email_string.length()];
  strcpy(full_email, full_email_string.c_str());
  computeDigest(full_email, strlen(full_email), digest);
  std::string hash;
  hash.reserve(32);
  for(int i = 0; i < 16; i++)
  {
    hash += "0123456789ABCDEF"[digest[i] / 16];
    hash += "0123456789ABCDEF"[digest[i] % 16];          
  }
  delete digest;
  return hash;
}

void getmails(std::string mail_str, std::vector<PennCloud::Mail> &mails) {
	int len = mail_str.length();
	int c = 0;
	while(c < len) {
		int size = stoi(mail_str.substr(c, c + 10));
		c += 10;
		std::string convert = mail_str.substr(c, c + size);
		PennCloud::Mail res;
		res.ParseFromString(convert);
		c += size;
		mails.push_back(res);
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



std::string serial_vector(std::vector<std::string> vec) {
	std::string s = "";
	for(int i = 0; i < vec.size(); i++) {
		s += vec[i];
		s += ",,,";
	}
	return s;
}

std::vector<std::string> deserial_vector(std::string str) {
std::vector<std::string> ret;
	std::size_t pos  = 0;
	while(pos < str.size()) {
		const std::size_t eq = str.find(",,,", pos);
		std::string key = str.substr(pos, eq - pos);
		ret.push_back(key);
		pos = eq + 3;

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


static std::unique_ptr<std::istream> get_emails(http::Response &resp) {
	http::Session &session = resp.get_session();
	session.set_username(user);
	if (session.get_username().empty()) {
		resp.resp_headers.emplace("Content-Type", "text/html");
		return std::make_unique<std::ifstream>("static/login.html");
	}
	std::string cur_user = session.get_username();
	std::string email_val = kvstore.get(cur_user, "__.mbox");
	getmails(email_val, emails);
	std::string table = "<table>  <tr> <th>To</th> <th>Date</th> <th>Subject</th><th>View</th></tr>";
	for(int i = 0; i < emails.size(); i++) {
		table += "<tr>";
		table += ("<td>" + emails[i].to() + "</td>");
		table += ("<td>" + emails[i].date() + "</td>");
		table += ("<td>" + emails[i].subject() + "</td>");
		char put[1000];
		sprintf(put, R"(<br /><a href="/read?index=%s">Read %s</a>)", urlEncode(emails[i].key()), "this email!");
		table +="<td>" + std::string(put) + "</td></tr>";
	}
	table += "</table>";
	std::string link =  R"(<br /><a href="/compose">Compose Email</a>)";
	std::unique_ptr<std::stringstream> ss = std::make_unique<std::stringstream>();
	//*ss << "Welcome, " << session.get_username() << "!";
	*ss << table << link;
	return ss;
}

static std::unique_ptr<std::istream> view_emails(http::Response &resp) {
	const std::unordered_map<std::string, std::string> params = resp.get_params();
	std::string username;
	if(params.find("index") == params.end()) {
			throw http::Exception(http::Status::BAD_REQUEST, "Bad Request");
	}
	int val = -1;
	std::string value = urlDecode(params.find("index")->second);
	for(int i = 0; i < emails.size(); i++) {
		if(value == emails[i].key()) {
			val = i;
		}
	}
	http::Session &session = resp.get_session();
	username = session.get_username();
	if (session.get_username().empty()) {
		resp.resp_headers.emplace("Content-Type", "text/html");
		return std::make_unique<std::ifstream>("static/login.html");
	}
	std::string message = kvstore.get(username, "__" + emails[val].key());
	std::unique_ptr<std::stringstream> ss = std::make_unique<std::stringstream>();
	*ss << message;
	return ss;
}

static std::unique_ptr<std::istream> send_emails(http::Response &resp) {
	std::unique_ptr<std::stringstream> ss = std::make_unique<std::stringstream>();
	std::ifstream file( "static/compose.html" );
	std::ifstream filse( "static/compose.html" );
    if ( file )
    {
	return std::make_unique<std::ifstream>("static/compose.html");
    }
	//*ss << "Welcome, " << session.get_username() << "!";
	return std::make_unique<std::ifstream>("static/compose.html");
}

static std::unique_ptr<std::istream> post_emails(http::Response &resp) {
	std::unordered_map<std::string, std::string> value = resp.parse_www_form();
	PennCloud::Mail putmail;
	std::string username, password, request_str;
	http::Session &session = resp.get_session();
	putmail.set_to(value["to"]);
	putmail.set_subject(value["subject"]);
	putmail.set_date("dateplacehold!");
	std::string hash = compute_hash(value["subject"], value["message"]);
	putmail.set_key("__" + hash);
	session.set_username(user);
	if (session.get_username().empty()) {
		resp.resp_headers.emplace("Content-Type", "text/html");
		return std::make_unique<std::ifstream>("static/login.html");
	}
	emails.push_back(putmail);
	username = session.get_username();
	putmail.SerializeToString(&request_str);
    char req_length[11];
    snprintf (req_length, 11, "%10d", request_str.length()); 
	std::string original = kvstore.get(username, "__.mbox");
	original += std::string(req_length) + request_str;
	kvstore.put(username, "__.mbox", original);
	kvstore.put(username, "__" + hash, value["message"]);
	std::unique_ptr<std::stringstream> ss = std::make_unique<std::stringstream>();
	//*ss << "Welcome, " << session.get_username() << "!";
	*ss << R"(<br /><a href="/">Go Back</a>)";
	return ss;
}





static void handle(int client) {
	try {
		http::handle_socket(client);
	} catch (const std::exception &e) {
		std::cerr << "Error: " << e.what() << std::endl;
	}
}

int main() {
	http::register_handler("/", http::Method::GET, get_emails);
	http::register_handler("/read", http::Method::GET, view_emails);
	http::register_handler("/compose", http::Method::GET, send_emails);
	http::register_handler("/postemail", http::Method::POST, post_emails);
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
	file = kvstore.get(user, "filename");
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