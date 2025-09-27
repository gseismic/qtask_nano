# qtask_nano/designs/simple

## Install

### MacOS
Install postgresql and start it.

```bash
brew install postgresql
brew services start postgresql

# "Run initdb or pg_bas..."点击查看元宝的回答
# https://yb.tencent.com/s/gIBD2crJNl9y
sudo mkdir -p /usr/local/var/postgresql@14
sudo chown -R $(whoami) /usr/local/var/postgresql@14
initdb -D /usr/local/var/postgresql@14 -E utf8 

psql -U $(whoami) -d postgres
```

Install redis and start it.
```bash
brew install redis
```

### Ubuntu


Install postgresql and start it.
```bash
sudo apt-get install postgresql
sudo systemctl start postgresql
```

Install redis and start it.
```bash
sudo apt-get install redis-server
sudo systemctl start redis-server
```

## Run

Run producer.py to produce tasks.
```bash
python producer.py
```

Run consumer.py to consume tasks.
```bash
python consumer.py
```

## Test

```bash
python test.py
```