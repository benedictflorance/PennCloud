#include "http.hpp"

#include <unistd.h>

#include <cstring>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <random>

#include <ext/stdio_filebuf.h>
#include <sys/socket.h>

#include "local_test.hpp"

KVstore kvstore;



namespace http {
std::unordered_map<std::string_view, std::unordered_map<Method, HandlerFunc>> handlers;

void register_handler(const char *const path, const Method method, const HandlerFunc &handler) {
	handlers[path][method] = handler;
}

static inline std::unique_ptr<std::istream> not_found_handler(Response &resp) { throw Exception(Status::NOT_FOUND); }

const static std::unordered_map<std::string_view, Method> method_map = {
	{"GET", Method::GET},
	{"POST", Method::POST},
};

void handle_socket(const int s) {
	const int ofd = dup(s);
	if (ofd < 0) {
		throw std::system_error(errno, std::generic_category(), "dup");
	}

	__gnu_cxx::stdio_filebuf<char> osocket(ofd, std::ios::out | std::ios::binary);
	if (!osocket.is_open()) {
		close(ofd);
		throw std::system_error(errno, std::generic_category(), "fdopen");
	}
	std::ostream ostream(&osocket);

	{
		static struct timeval timeout = {
			.tv_sec = 5,
			.tv_usec = 0,
		};
		if (setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
			throw std::system_error(errno, std::generic_category(), "setsockopt");
		}
	}
	__gnu_cxx::stdio_filebuf<char> socket(s, std::ios::in | std::ios::binary);
	if (!socket.is_open()) {
		close(s);
		throw std::system_error(errno, std::generic_category(), "fdopen");
	}
	std::istream istream(&socket);

	std::string init_req;
	if (!std::getline(istream, init_req) || init_req.empty() || init_req.back() != '\r') {
		// TODO?
		return;
	}
	init_req.pop_back();
	const std::string_view sv = init_req;

	const std::size_t space1 = sv.find(' ');
	if (space1 == std::string_view::npos) {
		// TODO?
		return;
	}

	const auto method_p = method_map.find(sv.substr(0, space1));
	if (method_p == method_map.end()) {
		// TODO?
		return;
	}
	const Method method = method_p->second;

	const std::size_t space2 = sv.find(' ', space1 + 1);
	if (space2 == std::string_view::npos) {
		// TODO?
		return;
	}

	const std::string_view path_with_params = sv.substr(space1 + 1, space2 - space1 - 1);
	std::size_t param_loc = path_with_params.find("?");
	std::string_view path = path_with_params.substr(0, param_loc);
	std::unordered_map<std::string, std::string> params;
	param_loc++;
	while(param_loc < path_with_params.size()) {
		const std::size_t eq = path_with_params.find('=', param_loc);
		if (eq == std::string::npos) {
			break;
		}
		const std::string_view key = path_with_params.substr(param_loc, eq - param_loc);
		param_loc = eq + 1;

		const std::size_t amp = path_with_params.find('&', param_loc);
		if (amp == std::string::npos) {
			params.emplace(std::move(key), path_with_params.substr(param_loc));
			break;
		}
		const std::string_view value = path_with_params.substr(param_loc, amp - param_loc);
		param_loc = amp + 1;

		params.emplace(std::move(std::string(key.begin(), key.end())), std::move(std::string(value.begin(), value.end())));
	}
	const std::string_view version = sv.substr(space2 + 1);
	if (version != "HTTP/1.1" && version != "HTTP/1.0") {
		ostream << "HTTP/1.0 " << Status::HTTP_VERSION_NOT_SUPPORTED << "\r\n";
		return;
	}

	std::string line;
	Headers headers;
	while (std::getline(istream, line)) {
		if (line.empty() || line.back() != '\r') {
			// TODO?
			return;
		}
		line.pop_back();
		if (line.empty()) {
			break;
		}
		const std::size_t colon = line.find(':');
		if (colon == std::string_view::npos) {
			// TODO?
			return;
		}
		std::string key = line.substr(0, colon);
		std::transform(key.begin(), key.end(), key.begin(), [](const char c) { return std::tolower(c); });
		std::string value = line.substr(colon + 1);
		value.erase(value.find_last_not_of(' ') + 1);
		value.erase(0, value.find_first_not_of(' '));

		headers.emplace(std::move(key), std::move(value));
	}

	Response response = {
		.method = method,
		.path = path,
		.req_headers = std::move(headers),
		.req_body = istream,
		.params = params
	};

	HandlerFunc handler = not_found_handler;
	const auto &it = handlers.find(path);
	if (it != handlers.end()) {
		const auto &it2 = it->second.find(method);
		if (it2 != it->second.end()) {
			handler = it2->second;
		}
	}

	std::unique_ptr<std::istream> resp_body;
	try {
		resp_body = handler(response);
	} catch (const http::Exception &e) {
		response.status = e.status;
		response.resp_headers.emplace("Content-Type", "text/plain");
		response.resp_headers.emplace("Content-Length", std::to_string(e.message.size()));
		resp_body = std::make_unique<std::istringstream>(e.message);
	}

	ostream << "HTTP/1.0 " << response.status << "\r\n";
	for (const auto &header : response.resp_headers)
		ostream << header.first << ": " << header.second << "\r\n";
	ostream << "\r\n";
	if (resp_body)
		ostream << resp_body->rdbuf();
}

const Status Status::OK = "200 OK";
const Status Status::FOUND = "302 Found";
const Status Status::BAD_REQUEST = "400 Bad Request";
const Status Status::FORBIDDEN = "403 Forbidden";
const Status Status::NOT_FOUND = "404 Not Found";
const Status Status::HTTP_VERSION_NOT_SUPPORTED = "505 HTTP Version Not Supported";
std::pair<Session &, bool> Session::get_session(const std::string &cookie) {
	const auto it = sessions.find(cookie);
	if (it != sessions.end()) {
		return std::make_pair(std::ref(it->second), true);
	}

	Session &s =
		sessions.emplace(std::piecewise_construct, std::forward_as_tuple(cookie), std::forward_as_tuple(cookie))
			.first->second;

	const std::string username = kvstore.get("SESSION", cookie);
	if (!username.empty()) {
		s.username = username;
		return std::make_pair(std::ref(s), true);
	}
	return std::make_pair(std::ref(s), false);
}

Session &Response::get_session() {
	const auto it = req_headers.find("cookie");

	if (it != req_headers.end()) {
		const std::string_view cookie = it->second;
		const std::size_t pos = cookie.find("session=");
		if (pos == std::string_view::npos) {
			goto no_cookie;
		}
		const std::string_view session = cookie.substr(pos + std::strlen("session="));
		const std::string session_id(session.substr(0, session.find(';')));

		if (session_id.size() != 16 || !std::all_of(session_id.begin(), session_id.end(), [](const char c) {
				return ('0' <= c && c <= '9') || ('a' <= c && c <= 'f');
			})) {
			goto no_cookie;
		}

		return Session::get_session(session_id).first;
	}
no_cookie:
	// generate new cookie
	std::random_device rd;
	std::mt19937_64 gen(rd());
	std::uniform_int_distribution<uint64_t> dis;

	std::string session_id;
	do {
		std::ostringstream ss;
		ss << std::hex << std::setfill('0') << std::setw(16) << dis(gen);
		session_id = ss.str();
	} while (Session::get_session(session_id).second);

	resp_headers.emplace("Set-Cookie", "session=" + session_id + "; Max-Age=86400; HttpOnly");
	return Session::get_session(session_id).first;
}

std::unordered_map<std::string, std::string> Response::get_params() {
	return params;
}

std::unordered_map<std::string, std::string> Response::parse_file_upload() {
	{
		const auto it = req_headers.find("content-type");
		if (it == req_headers.end()) {
			throw http::Exception(http::Status::BAD_REQUEST, "Content-Type must be  multipart/form-data");
		}
	}
	std::string content_type = req_headers.find("content-type")->second;
	const std::size_t b = content_type.find('=', 0);
	const std::string boundary = "--" + content_type.substr(b + 1);
	const std::size_t boundary_size = boundary.length();
	std::size_t content_length = 0;
	{
		const auto it = req_headers.find("content-length");
		if (it == req_headers.end()) {
			throw http::Exception(http::Status::BAD_REQUEST, "missing content-length");
		}
		content_length = std::stoul(it->second);
	}

	std::unordered_map<std::string, std::string> ret;
	std::vector<std::string> content;
	std::string multicontent;
	multicontent.resize(content_length);
	req_body.read(multicontent.data(), content_length);
	std::size_t pos = 0;
	while (pos < multicontent.size()) {
		const std::size_t bound = multicontent.find(boundary, pos);
		pos = bound + boundary_size + 2;
		const std::size_t bound2 = multicontent.find(boundary, pos);
		if(bound2 == std::string::npos) {
			content.push_back(multicontent.substr(pos));
			break;
		}
		else {
			content.push_back(multicontent.substr(pos, bound2 - pos));
		}

	}
	for(int i = 0; i < content.size(); i++) {
		pos = 0;
		std::size_t eq = content[i].find("filename=", pos);
		if (eq == std::string::npos) {
			break;
		}
		pos = eq + 1;
		const std::size_t amp = content[i].find("\r\n", pos);
		std::string value = content[i].substr(pos + 8, amp - pos);
		pos = amp + 2;
		ret.emplace(std::move("filename"), std::move(value));
		eq = content[i].find("\r\n\r\n", pos);
		value = content[i].substr(eq + 4);
		ret.emplace(std::move("content"), std::move(value));
	}

	return ret;
}


std::unordered_map<std::string, std::string> Response::parse_www_form() {
	{
		const auto it = req_headers.find("content-type");
		if (it == req_headers.end() || it->second != "application/x-www-form-urlencoded") {
			throw http::Exception(http::Status::BAD_REQUEST, "Content-Type must be application/x-www-form-urlencoded");
		}
	}
	std::size_t content_length = 0;
	{
		const auto it = req_headers.find("content-length");
		if (it == req_headers.end()) {
			throw http::Exception(http::Status::BAD_REQUEST, "missing content-length");
		}
		content_length = std::stoul(it->second);
	}

	std::unordered_map<std::string, std::string> ret;
	std::string content;
	content.resize(content_length);
	req_body.read(content.data(), content_length);

	std::size_t pos = 0;
	while (pos < content.size()) {
		const std::size_t eq = content.find('=', pos);
		if (eq == std::string::npos) {
			throw std::runtime_error("Invalid form data");
		}
		const std::string key = content.substr(pos, eq - pos);
		pos = eq + 1;

		const std::size_t amp = content.find('&', pos);
		if (amp == std::string::npos) {
			ret.emplace(std::move(key), content.substr(pos));
			break;
		}
		const std::string value = content.substr(pos, amp - pos);
		pos = amp + 1;

		ret.emplace(std::move(key), std::move(value));
	}

	return ret;
}

const std::string &Session::get_username() const { return username; }
void Session::set_username(const std::string &username) {
	kvstore.put("SESSION", session_id, username);
	this->username = username;
}

std::unordered_map<std::string, Session> Session::sessions;

} // namespace http
