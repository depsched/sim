PyWren runtime builders

To build all the runtimes in `runtimes.py` on the aws machine
`builder` and put them on the staging server: 

```
fab -f fabfile_builder.py -R builder build_all_runtimes 
```

To test, push the repo to github, which will trigger a travis build pointing
at the staged runtimes. 

To deploy them and shard them, do this:

```
fab -f fabfile_builder.py -R builder deploy_runtimes:num_shards=50
```

