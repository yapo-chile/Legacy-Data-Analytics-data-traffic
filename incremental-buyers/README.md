# incremental-buyers pipeline 

# incremental-buyers

## Description

Process migrate of incremental process that contain four phases:

- ods_buyer: get data from stg.ad_reply and ods.ad to extract information about new buyers and insert in table ods.buyers.

- ods_users_buyers: This step get data of buyers to complete dimension of users. In this process first delete data inserted in the day of processing in ods.users to clean in case of reprocessing, then get data of ods.buyers (inserted in last step) joined with ods.users, if the buyer is in ods.user, make an update of record, in other case, insert a new reg with buyer info.

- ods_users_sellers: This step get data of sellers to complete dimension of users. In this process get data of ods.sellers (inserted in last step) joined with ods.users, if the seller is in ods.user, make an update of record, in other case, insert a new register with buyer info. Finally, get registers of users with first_approval_date null, but joined with sellers with first_approval_date not null and make a update with this value in ods.users.


## Pipeline Implementation Details

|   Field           | Description                                                                |
|-------------------|----------------------------------------------------------------------------|
| Input Source      | stg.ad_reply, ods.ad, ods.buyer, ods.user, ods.seller                      |
| Output Source     | ods.user                                                                   |
| Schedule          | -:-                                                                        |
| Rundeck Access    | data jobs- CORE/Core - Buyers - User buyers and sellers                    |
| Associated Report | -                                                                          |


### Build
```
make docker-build
```

### Run micro services
```
docker run -v /local-path/secrets/pulse:/app/pulse-secret \
           -v /local-path/secrets/db-secret:/app/db-secret \
           -e APP_PULSE_SECRET=/app/pulse-secret \
           -e APP_DB_SECRET=/app/db-secret \
           containers.mpi-internal.com/yapo/incremental-buyers:[TAG]
```

### Run micro services with parameters

```
docker run -v /local-path/secrets/pulse:/app/pulse-secret \
           -v /local-path/secrets/db-secret:/app/db-secret \
           -e APP_PULSE_SECRET=/app/pulse-secret \
           -e APP_DB_SECRET=/app/db-secret \
           containers.mpi-internal.com/yapo/incremental-buyers:[TAG] \
           -date_from=YYYY-MM-DD \
           -date_to=YYYY-MM-DD
```

### Adding Rundeck token to Travis

If we need to import a job into Rundeck, we can use the Rundeck API
sending an HTTTP POST request with the access token of an account.
To add said token to Travis (allowing travis to send the request),
first, we enter the user profile:
```
<rundeck domain>:4440/user/profile
```
And copy or create a new user token.

Then enter the project settings page in Travis
```
htttp://<travis server>/<registry>/<project>/settings
```
And add the environment variable RUNDECK_TOKEN, with value equal
to the copied token
