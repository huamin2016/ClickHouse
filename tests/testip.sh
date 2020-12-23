#! /bin/bash
:> ~/testip.txt

for ip1 in `seq 255`;do
for ip2 in `seq 255`;do
for ip3 in `seq 5`;do
	echo $ip1"."$ip2"."$ip3"."$ip3 >>~/testip.txt
done
done
done

wc -l ~/testip.txt

clickhouse-client --query "insert into test_ip(ip) format CSV"<~/ testip.txt
