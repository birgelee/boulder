#pybgpstream install script
apt-get install libcurl3-dev
apt-get install libbz2-dev
apt-get install zlib1g-dev

mkdir ~/src
cd ~/src/
curl -O http://research.wand.net.nz/software/wandio/wandio-1.0.3.tar.gz
tar zxf wandio-1.0.3.tar.gz
cd wandio-1.0.3/
./configure
make
make install


cd ~/src/
curl -O http://bgpstream.caida.org/bundles/caidabgpstreamwebhomepage/dists/bgpstream-1.1.0.tar.gz
tar zxf bgpstream-1.1.0.tar.gz
cd bgpstream-1.1.0
./configure
make
make install