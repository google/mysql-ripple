# Ripple

Ripple is a server that can serve as a middleman in MySQL replication.

The Ripple server connects to a MySQL master the same way a replica would, but
rather than creating a copy of the data in MySQL, Ripple just downloads the
binlogs and saves them locally. MySQL servers can then be directed to replicate
from Ripple instead of the master. Ripple will serve downloaded binlogs to them
the same way the master would. This can significantly reduce load on the MySQL
master and improve durability of the binlogs.

Ripple supports replication to/from MariaDB and MySQL using GTIDs (of MariaDB
and MySQL flavor respectively). Replication using filename and position is not
supported. Ripple has been tested with MariaDB 10.0 and MySQL 5.6 and 5.7, but
it likely will work with later versions as well.

## Build instructions

Following build procedure works on Ubuntu 18.10 cloud image.

### Install bazel
```
apt-get install pkg-config zip g++ zlib1g-dev unzip python libssl-dev default-jdk-headless libmariadbclient-dev
echo "deb [arch=amd64] http://storage.googleapis.com/bazel-apt stable jdk1.8" | sudo tee /etc/apt/sources.list.d/bazel.list
curl https://bazel.build/bazel-release.pub.gpg | sudo apt-key add -
apt-get update && sudo apt-get install bazel
```

### Clone source
```
git clone https://github.com/google/mysql-ripple.git
```

### Build
```
cd mysql-ripple
bazel build :all
bazel test

```
If all went OK, your binary is at
```
./bazel-bin/rippled
```
