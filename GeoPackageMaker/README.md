# Basic Instructions

Create virtual env

```
mkdir venv
python3 -m venv ./venv
```

Activate virtual env

```
. ./venv/bin/activate
```

Install packages

```
. ./venv/bin/activate
pip3 install -r requirements.txt
```

*NB: Current package version of 'pygeopkg' writes out the date in the "gpkg_contents" table in a format that causes geoserver to fail but is fixed in github. (https://github.com/realiii/pygeopkg/commit/e3b9ca11d2dcc3239a2f498278906f776adf06af) There has been no release in pypi since, so we must edit the "geopkg.py" file to fix it.*

Change line 264 to:
```
return datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
``` 

Run script

```
./make_geopkg.py ./mscl12.gpkg
```
