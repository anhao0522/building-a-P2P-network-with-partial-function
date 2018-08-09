# building-a-P2P-network-with-partial-function

Python version: 3.6

Demo link: https://youtu.be/r8OSq7DCSRI

Description:
I create two threading to Ping successors and one threading for UDP server which can respond Ping request and receive the reply for Ping request.
Also, one threading is for TCP server that can receive and reply message sending by TCP. Last threading is for monitor standard input to get command.

For detection of the node that left the network, I used a counter and I just monitor each peer’s first successor. If node sends a Ping to first successor successfully , counter will add 1 and if node receives the response from first successor, counter will minus 1. So, if this node’s first successor left without notice, this will not get response, counter will increase. Once counter > 3, that means this node’s first successor is no longer alive. Then ask its second successor’s first successor and update that. Next, this node will notice its first predecessor that your second successor left the network and tell first predecessor’s new second successor identity.
