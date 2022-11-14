#include "http.hpp"

#include <unistd.h>

#include <iostream>
#include <sstream>
#include <thread>

#include <ext/stdio_filebuf.h>
#include <netinet/in.h>
#include <sys/socket.h>

static void index_page(http::Response &resp) {
	resp.resp_headers.emplace("Content-Type", "text/html");
	resp.resp_body = std::make_unique<std::ifstream>("static/index.html");
}

static void dynamic_content(http::Response &resp) {
	static int counter = 0;

	std::unique_ptr<std::stringstream> ss = std::make_unique<std::stringstream>();

	*ss << "You visited this page " << ++counter << " times.";

	resp.resp_body = std::move(ss);
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
	http::register_handler("/dynamic", http::Method::GET, dynamic_content);

	const int sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (sock == -1) {
		throw std::system_error(errno, std::generic_category(), "socket");
	}

	const sockaddr_in addr = {
		.sin_family = AF_INET,
		.sin_port = htons(12450),
		.sin_addr = {.s_addr = INADDR_ANY},
	};

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