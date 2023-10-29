1. Removed unnecessary assignment (context variable, blockhash, hash)
2. Merged three if statements into one (less code and more readable)
3. Removed if statement with error throw, beacuse it was always false
4. Removed of block check after api query, insead add additional query to api for block number to filter query data
5. Merged filter and slice data into one line
