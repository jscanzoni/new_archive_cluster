# New Cluster to Archive

This is a python script that:
- Creates an Atlas Cluster
- Loads the sample dataset into that cluster
- Runs the script from the beginning of the lab to fix dates
- Creates an online archive
- Connects to just the cluster to count un-archived documents
- Connects to just the online archive to count archived documents




## Instructions
The script runs off of a `.env` file with the following contents:


```bash
#contents of .env file

apiPub = "abcdefgh"
apiPriv = "11111111-1111-1111-1111-111111111111"
driverCreds = "user:pass"
groupId = "111111111111111111111111"
connectionStringSubdomain = "abcdef"

```
