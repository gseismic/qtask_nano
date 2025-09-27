# qtask_nano/designs/simple

## Install

### MacOS
Install postgresql and start it.
```bash
brew install postgresql
brew services start postgresql
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