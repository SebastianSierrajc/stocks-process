sudo apt update
sudo apt upgrade
sudo apt install openjdk-11-jdk-headless

rm -r kafka_2.13-3.2.0.tgz kafka_2.13-3.2.0

wget https://dlcdn.apache.org/kafka/3.2.0/kafka_2.13-3.2.0.tgz

tar -xzf kafka_2.13-3.2.0.tgz
cd kafka_2.13-3.2.0

bin/zookeeper-server-start.sh config/zookeeper.properties &
bin/kafka-server-start.sh config/server.properties &

jobs -l

ip4=$(/sbin/ip -o -4 addr list eth0 | awk '{print $4}' | cut -d/ -f1)
port=9092
topic=stocks-process

bin/kafka-topics.sh --create --topic $topic --bootstrap-server $ip4:$port &
