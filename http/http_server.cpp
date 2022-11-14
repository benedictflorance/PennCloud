#include "http.hpp"

#include <unistd.h>

#include <iostream>

#include <ext/stdio_filebuf.h>
#include <sys/socket.h>

namespace http {
std::unordered_map<std::string_view, std::unordered_map<Method, HandlerFunc>> handlers;

void register_handler(const char *const path, const Method method, const HandlerFunc &handler) {
	handlers[path][method] = handler;
}

static inline void not_found_handler(Response &resp) { resp.status = Status::NOT_FOUND; }

const static std::unordered_map<std::string_view, Method> method_map = {
	{"GET", Method::GET},
	{"POST", Method::POST},
};

void handle_socket(const int s) {
	{
		static struct timeval timeout = {
			.tv_sec = 5,
			.tv_usec = 0,
		};
		if (setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
			throw std::system_error(errno, std::generic_category(), "setsockopt");
		}
	}

	const int ofd = dup(s);
	if (ofd < 0) {
		throw std::system_error(errno, std::generic_category(), "dup");
	}

	__gnu_cxx::stdio_filebuf<char> socket(s, std::ios::in | std::ios::binary);
	if (!socket.is_open()) {
		throw std::system_error(errno, std::generic_category(), "fdopen");
	}
	std::istream istream(&socket);

	__gnu_cxx::stdio_filebuf<char> osocket(ofd, std::ios::out | std::ios::binary);
	if (!osocket.is_open()) {
		throw std::system_error(errno, std::generic_category(), "fdopen");
	}
	std::ostream ostream(&osocket);

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

	const std::string_view path = sv.substr(space1 + 1, space2 - space1 - 1);

	const std::string_view version = sv.substr(space2 + 1);
	if (version != "HTTP/1.1" && version != "HTTP/1.0") {
		ostream << "HTTP/1.0 " << Status::HTTP_VERSION_NOT_SUPPORTED << "\r\n";
		return;
	}

	std::string line;
	while (std::getline(istream, line)) {
		if (line.empty() || line.back() != '\r') {
			// TODO?
			return;
		}
		line.pop_back();
		if (line.empty()) {
			break;
		}
	}

	Response response = {
		.method = method,
		.path = path,
		.req_body = istream,
	};

	HandlerFunc handler = not_found_handler;
	const auto &it = handlers.find(path);
	if (it != handlers.end()) {
		const auto &it2 = it->second.find(method);
		if (it2 != it->second.end()) {
			handler = it2->second;
		}
	}

	handler(response);

	ostream << "HTTP/1.0 " << response.status << "\r\n";
	ostream << "\r\n";
	if (response.resp_body) {
		ostream << response.resp_body->rdbuf();
	}
}

const Status Status::OK = "200 OK";
const Status Status::NOT_FOUND = "404 Not Found";
const Status Status::HTTP_VERSION_NOT_SUPPORTED = "505 HTTP Version Not Supported";

} // namespace http
