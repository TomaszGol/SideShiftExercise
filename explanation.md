1. Removed an unnecessary assignment (context variable).
2. Merged three if statements into one (less code and improved readability).
3. Removed an if statement with an error throw because it was always false.
4. Removed a block check after api query, instead added additional query to the api for block number to filter query data.
5. Merged the filter and slice operations into one line.
6. Moved the try-catch block to a function with api query.
7. Added type annotations where type information is available.
8. Moved asserts and validation to separate functions.