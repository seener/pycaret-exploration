# pycaret-exploration
Playing around with pycaret to see how it works. Also going to do a little bit of the data engineering work with dbt. Might even build some custom tests :-). 

## Data Sources
- [Table 14-10-0090-01 Labour force characteristics by province, territory and economic region, annual](https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1410009001)
- [Table 14-10-0092-01 Employment by industry, annual, provinces and economic regions (x 1,000)](https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1410009201)
- [Table 14-10-0312-01 Employment by economic regions and occupation, annual (x 1,000)](https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1410031201)

## Environment build
**Build the virtual environment with proper dependencies**
1. Create the conda virtual environment for analytics: $`conda env create -f environment.yml`
2. Activate the conda environemnt and create analytics kernal: $`conda activate pce-env` 
3. Create an ipython kernal for the environment $`ipython kernel install --name "k-pce" --user`

```
conda env create -f environment.yml
conda activate pce-env
ipython kernel install --name "k-pce" --user

```

**Make sure that ~/.dbt/profiles.yml has the correct content**

`~/.dbt/profiles.yml` should have the following content: 
```
exploration:
  outputs:
    dev:
      type: postgres
      threads: 1
      host: localhost
      port: 5432
      user: [SomeUserNameHere]
      pass: [SomePasswordHere]
      dbname: analytics
      schema: dev
  target: dev
```

**If you need to recreate the folder structure and baseline files**
1. Initialize the dbt structure with: `dbt init temp`
2. Remove the readme because we'll use the existing one: `rm temp/README.md`
3. Remove the example content because we don't need it: `rm temp/models/example/*` and `rmdir temp/models/example`
4. Move all of the folders and files up a level: `mv temp/* ./`
5. Move the gitignore: `mv temp/.gitignore ./`
5. Remove the temp: `rmdir temp`
6. Add a gitkeep file to models: `touch models/.gitkeep`
7. Add the packages: `dbt deps`
8. Add model folders: `mkdir models/atomic`, `mkdir models/stage`, and `mkdir models/analytic` 

```
dbt init temp

rm temp/README.md
rm temp/models/example/*
rmdir temp/models/example
mv temp/* ./
mv temp/.gitignore ./
rmdir temp
touch models/.gitkeep

dbt deps

mkdir models/atomic 
touch models/atomic/.gitkeep
mkdir models/stage
touch models/stage/.gitkeep
mkdir models/analytic
touch models/analytic/.gitkeep

```
