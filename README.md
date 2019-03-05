Software to reconstruct git history from tarballs.

https://marc.info/?l=linux-kernel&m=139033182525831

# Installing

## Ubuntu 16.04

The Git package in Xenial is too old, so you'll need to upgrade it.

```sh
git clone https://github.com/ali1234/gitxref.git
cd gitxref/
sudo python3 setup.py install

sudo add-apt-repository ppa:git-core/ppa
sudo apt-get update
sudo apt-get install git
```
