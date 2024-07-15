# Multithreaded HTTP Server

This program implements a HTTP Server which runs continuously on multiple threads. It recieves requests, parses them, and returns a response, delivering multiple errors. This program can parse GET and PUT requests.

## Building

Build the Program with:

$ make 

## Running

Run the program with:

$ ./httpserver (-t threads) (port)

Once the HTTP server is running, one can feed in requests through netcat, curl, etc. The program will recieve the request, complete the method, and send a response back to the client. The -t optional flag specifies the amount of threads to be created. The default is 4.

**This project was created in UCSC's CSE 130 Principles of Computer System Design Course by Andrew Quinn / Kerry Veenstra**