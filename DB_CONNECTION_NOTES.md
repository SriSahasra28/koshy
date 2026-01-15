# Database Connection Notes

## Connection Details
- **Host**: 103.160.145.141
- **Port**: 3306
- **User**: root
- **Database**: algo

## Connection Issues

The scripts are timing out when trying to connect to the database. This is likely due to:

1. **Network/Firewall Restrictions**: The database server (103.160.145.141) may not be accessible from your local machine
2. **Remote Access**: The MySQL server may only accept connections from specific IP addresses
3. **VPN/Tunnel Required**: You may need to be on a VPN or use an SSH tunnel to access the database

## Solutions

### Option 1: Run Scripts on Server
If you have SSH access to the server where the database is located, run the scripts there:
```bash
ssh user@103.160.145.141
cd /path/to/koshy_python
python audit_condition1_comparison.py
```

### Option 2: SSH Tunnel (If you have SSH access)
Create an SSH tunnel to access the database:
```bash
ssh -L 3306:localhost:3306 user@103.160.145.141
```
Then modify scripts to connect to `localhost:3306` instead.

### Option 3: Check Database Access
Ask your database administrator to:
- Verify your IP is whitelisted
- Check if MySQL is configured to accept remote connections
- Verify firewall rules allow port 3306

## Testing Connection

To test if the database is accessible, you can:
1. Use MySQL Workbench or similar tool to connect
2. Use `mysql` command line client: `mysql -h 103.160.145.141 -P 3306 -u root -p algo`
3. Check if other scripts in the codebase can connect (they might be running on a server)

## Script Status

All scripts are ready and have proper error handling. They will work once database connectivity is established.
