brew install hadoop
mkdir -p /opt/yarn/
cd /opt/yarn
mkdir -p /etc/profile.d/java.sh
echo "export JAVA_HOME=/usr/libexec/java_home" > /etc/profile.d/java.sh
source /etc/profile.d/java.sh
mkdir -p /var/data/hadoop/hdfs/nn
mkdir -p /var/data/hadoop/hdfs/snn
mkdir -p /var/data/hadoop/hdfs/dn
mkdir -p /var/log/hadoop/yarn