# Master Server:

- How to launch the master?
> `./master` should be sufficient. Master supports -v flag for verbose.

- How to establish connection with the master?
> Master has a TCP Socket open at address in the \<MASTER> section of config file - `./configs/tablet_server_config.txt`

- Commands supported and specification
> Frontend server and the supported plugins need to explicitly request the rowkey from master using the command - REQ(rowkey)

- Response
> Master will then provide them the address of the relevant tablet server (in the form of a string) which may or may not be the primary

# Tablet Server:

- How to launch the tablet?
> `./tablet_server -v config_file_path tablet_server_index` For example, `./tablet_server -v configs/tablet_server_config.txt 0`
Tablet server indices are 0-indexed.

- How to establish connection with the tablet?
> Tablet has a TCP Socket open at address in the config file - `./configs/tablet_server_config.txt` (Look for the server index under \<TABLETS>)

- What's the format of the config file?
> Line under \<MASTER> has master address, lines under <TABLETS> have tablet address and lines under <REPLICAS> are of the format `startchar-endchar,server_idx_1,server_idx_2,...` where starchar is the start of the rowkey range, endchar is the end of the rowkey range and the following indices are the server indices that handle requests for that rowkey range, with the first server being the primary of that rowkey
range.

- How to communicate to the tablet server?
> All communication (both requests and responses) to the tablet server must be serialized through Google's 
Protobuf. Learn more [here](https://developers.google.com/protocol-buffers/docs/overview).

- How to setup Protobuf?
>If you're an M1 user, follow this
```
/bin/bash -c “$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
export PATH=/opt/homebrew/bin:$PATH
git clone https://github.com/protocolbuffers/protobuf.git
git checkout 96ccf40
cd protobuf
brew install autoconf
brew install automake
brew install Libtool
autoreconf -i
./autogen.sh
./configure
make
make check
sudo make install
export PATH=/opt/usr/local/bin:$PATH
```

> If you're not an M1 user, follow the documentation [here](https://github.com/protocolbuffers/protobuf/tree/main/src)

- Commands supported and specification

> All commands are case-insensitive (i.e., both CPUT or cPuT will work). Each request should be formatted in accordance with request.proto and each response from the tablet server will be formatted in accordance to response.proto. The frontend server should serialize according to request.proto before sending the request and de-serialize the response using response.proto. Tablet server uses "\r\n" as the delimiter to separate consecutive commands. `tablet_client.cpp` already has a mock client, and one can emulate it to send requests to the tablet server. Tablet client is launched using the same arguments as tablet server.

- Response
> Response will have a status ("+OK" or "-ERR"), a description if there's an error and a value if the command response must have a value.