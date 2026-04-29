## Note
This project assumes PostgreSQL is configured for local development access.
in postgresql.conf
```bash
# Change from:
#listen_addresses = 'localhost'
# To:
listen_addresses = '*'
```
in pg_hba.conf
```
# Allow passwordless TCP connections from any IP (LOCAL DEV ONLY)
host    all             all             0.0.0.0/0               trust
host    all             all             ::/0                    trust
```
open firewal
```
port 5432/tcp # commands vary as per distro
```
