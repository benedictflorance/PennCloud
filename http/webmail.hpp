#include "http.hpp"
#include <unistd.h>
#include "../kvstore/local_test.hpp"
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
#include <iomanip>

std::string user = "0";
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

std::pair<std::string, std::string> split_email(std::string email) {
	email = email.substr(0, email.length() - 3); // Remove >\r\n
	int at_index = email.find('@');
	if(email.length() < 3 || count(email.begin(), email.end(), '@') != 1 || 
			email.find('@') == email.length() - 1 || email.find('@') == std::string::npos
			|| email.find(' ') != std::string::npos) {
				return std::pair<std::string, std::string>();
			} // Email should atleast have letter@letter
	std::string username = email.substr(0, at_index-0);
	std::string hostname = email.substr(at_index+1, email.length()-at_index-1);
	return std::pair<std::string, std::string>(username, hostname);
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




static std::unique_ptr<std::istream> post_data(http::Response &resp) {
	const std::unordered_map<std::string, std::string> form = resp.parse_www_form();
	for (auto it = form.begin(); it != form.end(); it++) {
		std::cout << it->first << " " << it->second << std::endl;
	}
	return nullptr;

}



static std::unique_ptr<std::istream> list_emails(http::Response &resp) {
	if (resp.session.get_username().empty()) {
		resp.resp_headers.emplace("Content-Type", "text/html");
		return std::make_unique<std::ifstream>("static/login.html");
	}
	std::string cur_user = resp.session.get_username();
	std::string email_val = kvstore.get(cur_user, "__.mbox");
	getmails(email_val, emails);
	std::string json = "[";
	for(int i = 0; i < emails.size(); i++) {
		char put[1000];
		sprintf(put, R"({"to" : %s, "subject" : "%s",  "date" : "%s", "key" : "%s"})", emails[i].to(), emails[i].subject(), 
		emails[i].date(), urlEncode(emails[i].key()));
		json += put;
		if(i != emails.size() - 1) json += ",";
	}
	json += "]";
	std::string link =  R"(<br /><a href="/compose">Compose Email</a>)";
	std::unique_ptr<std::stringstream> ss = std::make_unique<std::stringstream>();
	//*ss << "Welcome, " << session.get_username() << "!";
	*ss << json;
	return ss;
}

static std::unique_ptr<std::istream> get_email(http::Response &resp) {
	const std::unordered_map<std::string, std::string> params = resp.get_params();
	std::string username;
	if(params.find("index") == params.end()) {
			throw http::Exception(http::Status::BAD_REQUEST, "Bad Request");
	}
	int val = -1;
	std::string value = urlDecode(params.find("index")->second);
	if (resp.session.get_username().empty()) {
		resp.resp_headers.emplace("Content-Type", "text/html");
		return std::make_unique<std::ifstream>("static/login.html");
	}
	username = resp.session.get_username();
	std::string message = kvstore.get(username, "__" + value);
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

static std::unique_ptr<std::istream> send_email(http::Response &resp) {
	resp.session.set_username(user);
	std::string username = resp.session.get_username();
	if (resp.session.get_username().empty()) {
		resp.resp_headers.emplace("Content-Type", "text/html");
		return std::make_unique<std::ifstream>("static/login.html");
	}
   	time_t now = time(0);
  	char* dt = ctime(&now);
	std::unordered_map<std::string, std::string> value = resp.parse_www_form();
	PennCloud::Mail putmail;
	std::string password, request_str;
	std::pair<std::string, std::string> email = split_email(value["to"]);
	putmail.set_to(email.first);
	putmail.set_subject(value["subject"]);
	putmail.set_date(dt);
	putmail.set_from(username);
	std::string hash = compute_hash(value["subject"], value["message"]);
	putmail.set_key(hash);
	emails.push_back(putmail);
	putmail.SerializeToString(&request_str);
    char req_length[11];
    snprintf (req_length, 11, "%10d", request_str.length()); 
	std::string original = kvstore.get(email.first, "__.mbox");
	original += std::string(req_length) + request_str;
	kvstore.put(email.first, "__.mbox", original);
	kvstore.put(email.first, "__" + hash, value["message"]);
	std::unique_ptr<std::stringstream> ss = std::make_unique<std::stringstream>();
	*ss << R"(<br /><a href="/">Go Back</a>)";
	return ss;
}

static std::unique_ptr<std::istream> delete_email(http::Response &resp) {
	const std::unordered_map<std::string, std::string> params = resp.get_params();
	std::string username;
	if(params.find("index") == params.end()) {
			throw http::Exception(http::Status::BAD_REQUEST, "Bad Request");
	}
	int val = -1;
	std::string value = urlDecode(params.find("index")->second);
	if (resp.session.get_username().empty()) {
		resp.resp_headers.emplace("Content-Type", "text/html");
		return std::make_unique<std::ifstream>("static/login.html");
	}
	username = resp.session.get_username();
	kvstore.dele(username, "__" + value);
	std::string email_val = kvstore.get(username, "__.mbox");
	getmails(email_val, emails);
	int index = -1;
	for(int i = 0; i < emails.size(); i++) {
		if(emails[i].key() == value) {
			index = i;
			break;
		}
	}
	if (index == -1) return NULL;
	std::unique_ptr<std::stringstream> ss = std::make_unique<std::stringstream>();
	*ss << "Deleted!";
	return ss;
}




