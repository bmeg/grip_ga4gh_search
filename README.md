
# GRIP GA4GH Search API plugin

Prototype GRIPPER module to allow GRIP to connect to proposed 
[GA4GH search API](https://github.com/ga4gh-discovery/ga4gh-search). 

This prototype is targeted to GRIP 0.7.0, which is currently in 
[pre-release development](https://github.com/bmeg/grip/tree/develop-0.7.0). 


## Building server
```
git clone git@github.com:bmeg/grip_ga4gh_search.git
cd grip_ga4gh_search
go build ./
```

## Test client by listing tables on GA4GH search server
```
./grip_ga4gh_search list https://search-presto-public.prod.dnastack.com/
```

## Build config file template
```
./grip_ga4gh_search gen-config https://search-presto-public.prod.dnastack.com/ > dnastack.config.yaml
```

NOTE: Because of the GRIP data model, every element is required to have a primary key. If 
the config file has an empty primaryKey field in a table description, it will not 
be presented by the server. 

## Turn on server
```
./grip_ga4gh_search server dnastack.config.yaml
```

## Build GRIP 0.7.0 development branch
```
git clone git@github.com:bmeg/grip.git
cd grip/
git checkout develop-0.7.0
go build ./
```

## List available tables
```
./grip er list
```

## Dump all rows from a table
```
./grip er rows kidsfirst.ga4gh_tables.specimen
```
