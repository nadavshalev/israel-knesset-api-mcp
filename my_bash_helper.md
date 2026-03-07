# ----------------------------------------
# Essentials
## activate the virtual environment
```bash
source .venv/bin/activate
```

## run Hebrew support terminal
```bash
konsole &
```

## run sqlite browser
```bash
sqlitebrowser data.sqlite
```

## run all tests
```bash
pytest -v
```

# ------------------------------------------
# MCP
## run the MCP server
```bash
python mcp_server.py 
```

## run MCP Inspection Tool
```bash
npx @modelcontextprotocol/inspector
```

# ------------------------------------------
# PostgreSQL
## Connect Commands
* SSH into the PostgreSQL server: `ssh -p 22222 service_hudmhfx6yd2u@default-server-u5xbns.sliplane.app`

* run psql: `psql -h localhost -U postgres -d mydb -W`

## psql commands
* list databases: `\l`
* connect to a database: `\c mydb`
* list tables: `\dt+`
* describe a table: `\d+ table_name`
* run a query: `SELECT * FROM table_name LIMIT 10;`
* exit psql: `\q`

# ------------------------------------------
# Docker
## Commands
* Run docer-compose: `sudo docker compose up -d --build`
* View logs in real-time: `sudo docker compose logs -f`
* Stop containers: `sudo docker compose down`
* Restart containers: `sudo docker compose restart`

