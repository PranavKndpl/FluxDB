# FluxDB Python Driver

Official driver for connecting to [FluxDB](https://github.com/your-username/FluxDB).

## Installation
```bash
pip install fluxdb-driver
```
## Usage

from fluxdb import FluxDB

# Connect (Default: 127.0.0.1:8080)
db = FluxDB(password="flux_admin")

# Create/Switch Database
db.use("game_data")

# Insert
uid = db.insert({"username": "Player1", "score": 100})

# Smart Query
results = db.find({"score": {"$gt": 50}})
print(results)