avl:
	g++ -Wall -Wextra -O2 -g test_offset.cpp -o test_offset
	g++ -Wall -Wextra -O2 -g 11_server.cpp hashtable.cpp zset.cpp avl.cpp -o server
	g++ -Wall -Wextra -O2 -g 11_client.cpp -o client 
	# g++ -Wall -Wextra -O2 -g test_h_insert.cpp hashtable.cpp zset.cpp avl.cpp -o test_h_insert
clean:
	rm -rf test_offset server client test_h_insert