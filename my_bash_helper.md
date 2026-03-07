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
## SSH into the PostgreSQL server
```bash
ssh -p 22222 service_hudmhfx6yd2u@default-server-u5xbns.sliplane.app
```

## connect to the PostgreSQL database using psql
```bash
psql -h localhost -U postgres -d mydb -W
```

## psql commands
* list databases: `\l`
* connect to a database: `\c mydb`
* list tables: `\dt+`
* describe a table: `\d+ table_name`
* run a query: `SELECT * FROM table_name LIMIT 10;`

