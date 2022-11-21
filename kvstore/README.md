# Master Server:

- How to launch the master?
> ./master should be sufficient. Master supports -v flag for verbose and -p for specifying the port number

- How to establish connection with the master?
> Master has a TCP Socket open at address in the config file - kvstore/configs/tablet_server_config.txt

- Commands supported and specification
> Frontend server and the supported plugins need to explicitly request the rowkey from master using the command - REQ(rowkey)

- Response
> Master will then provide them the address of the relevant tablet server (in the form of a string) which may or may not be the primary

# Tablet Server:

- How to launch the tablet?

- How to establish connection with the tablet?

- What's the format of the config file?

- Commands supported and specification

- Response