#include <algorithm>
#include <fstream>
#include <iostream>
#include <iterator>
#include <mutex>
#include <thread>
#include <vector>

#include <ext/stdio_filebuf.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

static inline std::vector<std::uint16_t> parse_ports(const std::string &path) {
	std::ifstream config(path);
	if (!config) {
		throw std::runtime_error("Cannot open config file");
	}
	std::vector<std::uint16_t> servers;
	std::copy(std::istream_iterator<std::uint16_t>(config), std::istream_iterator<std::uint16_t>(),
			  std::back_inserter(servers));
	if (!config.eof()) {
		throw std::runtime_error("Invalid config file");
	}
	for (const auto &s : servers) {
		if (s == 0) {
			throw std::runtime_error("Invalid port");
		}
	}
	return servers;
}

static class Info {
	std::mutex lock;

  public:
	std::vector<std::uint16_t> status;
	std::vector<std::uint16_t> list_server_status() {
		std::lock_guard<std::mutex> guard(lock);
		return status;
	}
	void set_server_status(std::size_t index, std::uint16_t s) {
		std::lock_guard<std::mutex> guard(lock);
		status[index] = s;
	}
	std::size_t choose_best_server() {
		std::lock_guard<std::mutex> guard(lock);
		auto it = std::min_element(status.begin(), status.end());
		if (*it != (std::uint16_t)-1)
			++*it;
		return it - status.begin();
	}
} info;

static void connect_to_servers(std::size_t index, const std::uint16_t port) {
	// dial localhost port
	info.set_server_status(index, -1);
	const int s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (s == -1) {
		throw std::system_error(errno, std::generic_category(), "socket");
	}

	struct sockaddr_in addr = {
		.sin_family = AF_INET,
		.sin_port = htons(port),
		.sin_addr = {htonl(INADDR_LOOPBACK)},
	};

	if (connect(s, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) == -1) {
		goto retry;
	}

	while (true) {
		char buf[2];
		if (read(s, buf, 1) != 1)
			goto retry;
		if (buf[0] == '0') {
			if (read(s, buf, 2) != 2)
				goto retry;
			info.set_server_status(index, ntohs(*(reinterpret_cast<std::uint16_t *>(buf))));
		} else if (buf[0] == '1') {
			const auto status = info.list_server_status();
			*(reinterpret_cast<std::uint16_t *>(buf)) = htons(status.size());
			if (write(s, buf, 2) != 2)
				goto retry;
			for (std::uint16_t i = 0; i < status.size(); ++i) {
				*(reinterpret_cast<std::uint16_t *>(buf)) = htons(status[i]);
				if (write(s, buf, 2) != 2)
					goto retry;
			}
		}
	}
retry:
	close(s);
	std::this_thread::sleep_for(std::chrono::seconds(2));
	return connect_to_servers(index, port);
}

std::vector<std::uint16_t> servers;

static void handle(const int s) {
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
		return;
	}
	init_req.pop_back();
	const std::string_view sv = init_req;

	const std::size_t space1 = sv.find(' ');
	if (space1 == std::string_view::npos) {
		return;
	}

	const std::size_t space2 = sv.find(' ', space1 + 1);
	if (space2 == std::string_view::npos) {
		return;
	}

	const std::string_view path_with_params = sv.substr(space1 + 1, space2 - space1 - 1);
	ostream << "HTTP/1.0 307 Temporary Redirect\r\n"
			<< "Location: http://localhost:" << servers[info.choose_best_server()] << path_with_params << "\r\n\r\n";
}

int main(int argc, char **argv) {
	if (argc != 3) {
		std::cerr << "Usage: " << argv[0] << " <port> <config file>" << std::endl;
		return 1;
	}
	const uint16_t port = std::stoi(argv[1]);

	servers = parse_ports(argv[2]);
	info.status.resize(servers.size(), -1);
	for (std::size_t i = 0; i < servers.size(); ++i) {
		std::thread(connect_to_servers, i, servers[i] + 10000).detach();
	}

	const int sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (sock == -1) {
		throw std::system_error(errno, std::generic_category(), "socket");
	}

	const sockaddr_in addr = {
		.sin_family = AF_INET,
		.sin_port = htons(port),
		.sin_addr = {.s_addr = INADDR_ANY},
	};

	if (bind(sock, reinterpret_cast<const sockaddr *>(&addr), sizeof(addr)) == -1) {
		throw std::system_error(errno, std::generic_category(), "bind");
	}

	if (listen(sock, 10) == -1) {
		throw std::system_error(errno, std::generic_category(), "listen");
	}

	std::cerr << "Listening on port " << port << std::endl;
	while (true) {
		const int client = accept(sock, nullptr, nullptr);
		if (client == -1) {
			throw std::system_error(errno, std::generic_category(), "accept");
		}

		std::thread(handle, client).detach();
	}
}