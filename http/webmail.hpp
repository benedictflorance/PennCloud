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
#define MAX_PUT 10000
#define MAX_BUF 1000
std::string user = "0";
const std::string files = "_files"; 
std::vector<std::string> filelist;
std::vector<PennCloud::Mail> emails;

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
	std::vector<std::string> emails = kvstore.list_colkeys("MAILBOX_" + cur_user);
	std::string json = "[";
	for(int i = 0; i < emails.size(); i++) {
		char put[1000];
		std::vector<std::string> splitted;
		 split(emails[i], " ", splitted);
		sprintf(put, R"({"from" : "%s", "subject" : "%s",  "time" : "%s"})", splitted[0], splitted[1], 
		splitted[2]);
		json += put;
		if(i != emails.size() - 1) json += ",";
	}
	json += "]";
	std::unique_ptr<std::stringstream> ss = std::make_unique<std::stringstream>();
	*ss << json;
	return ss;
}

static std::unique_ptr<std::istream> get_email(http::Response &resp) {
	std::string username = resp.session.get_username();
	std::string index;
	resp.req_body >> index;
	int val = -1;
	char put[MAX_PUT];
	std::string email_buf = kvstore.get("MAILBOX_" + username, index);
	PennCloud::Mail email;
	email.ParseFromString(email_buf);
	sprintf(put, R"({"from" : "%s", "to": "%s", "subject" : "%s",  "time" : "%s", "content": "%s})", 
	email.from().c_str(), email.to().c_str(), email.subject().c_str(), email.time().c_str(), email.content().c_str());
	std::unique_ptr<std::stringstream> ss = std::make_unique<std::stringstream>();
	*ss << put;
	return ss;
}


static std::unique_ptr<std::istream> send_email(http::Response &resp) {
	char buf[MAX_BUF];
	std::vector<std::string> email_vec;
	std::string subject, emails, content;
	std::string username = resp.session.get_username();
	resp.req_body.getline(buf, MAX_BUF);
	emails = buf;
	resp.req_body.getline(buf, MAX_BUF);
	subject = buf;
	std::ostringstream std_input;
	std_input << resp.req_body.rdbuf();
	content = std_input.str();
	split(emails, " ", email_vec);
	if (resp.session.get_username().empty()) {
		resp.resp_headers.emplace("Content-Type", "text/html");
		return std::make_unique<std::ifstream>("static/login.html");
	}
   	time_t now = time(0);
  	char* dt = ctime(&now);
	PennCloud::Mail putmail;
	std::string password, request_str, ckey;
	for(int i = 0; i < email_vec.size(); i++) {
		std::pair<std::string, std::string> to_email = split_email(email_vec[i]);
		putmail.set_to(email_vec[i]);
		putmail.set_subject(subject);
		putmail.set_time(dt);
		putmail.set_from(username + "@localhost");
		putmail.set_content(content);
		ckey = putmail.from() + " " + putmail.subject() + " " + putmail.time();
		putmail.SerializeToString(&request_str);
    	char req_length[11];
    	snprintf (req_length, 11, "%10d", request_str.length()); 
		if(to_email.second == "localhost") {
			kvstore.put("MAILBOX_" + to_email.first, ckey, request_str);
		}
		else {
			kvstore.put("MAILBOX_.mqueue", ckey, request_str);
		}
	}
	std::unique_ptr<std::stringstream> ss = std::make_unique<std::stringstream>();
	*ss << R"(<br /><a href="/">Go Back</a>)";
	return ss;
}

static std::unique_ptr<std::istream> delete_email(http::Response &resp) {
	std::string username = resp.session.get_username();
	std::string index;
	resp.req_body >> index;
	int val = -1;
	kvstore.dele("MAILBOX_" + username, index);
	std::unique_ptr<std::stringstream> ss = std::make_unique<std::stringstream>();
	*ss << "Deleted!";
	return ss;
}




