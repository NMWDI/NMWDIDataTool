# NMWDIDataTool
A python based tool for accessing data in the NMWDI catalog. Uses PySTA to query SensorThings instances. 
The application logic in order integrate other APIs exists here also


# Install

```sh
pip install nmwdidatatool
```

# Usage
```sh
nmwdi --help
```

# Unannotated Examples
### Water
```
nmwdi water depths --agency CABQ --location YALE* --out out.csv
nmwdi water depths --agency CABQ --location YALE* --out foo.json
nmwdi water depths --agency CABQ --location YALE* --out out.json --last 2
nmwdi water depths --agency CABQ --location YALE* --out out.csv --last 2
nmwdi water elevations --agency CABQ --location YALE* --out out.csv --last 2
```
###Locations
```
nmwdi locations --within "bernalillo" --verbose --out foo.shp --group True
nmwdi locations --within bernallilo --names-only
nmwdi locations --within bernalillo --names-only --agency CABQ
nmwdi locations --pages 1 --bbox "-108 35, -106 34.9" --verbose --screen
nmwdi locations --pages 1 --bbox "-108 35, -106 34.9" --verbose --screen --out foo.json
nmwdi locations --pages 1 --name "startswith(name, 'AR-')"
nmwdi locations --pages 1 --name "startswith(name, 'AR-')" --verbose
nmwdi locations --pages 1 --agency ISC_SEVEN_RIVERS --verbose
nmwdi locations --pages 1 --agency ISC_SEVEN_RIVERS --verbose --expand Things/Datastreams --screen
nmwdi locations --pages 1 --within "NM:Socorro" --verbose
nmwdi locations --pages 1 --within "NM:Socorro" --verbose --screen
nmwdi locations --pages 1 --within "NM:Socorro" --verbose --screen --expand Things/Datastreams
nmwdi locations --pages 1 --within "NM:Socorro" --verbose --out foo.csv
nmwdi locations --pages 1 --within "NM:Socorro" --verbose --url ose.newmexicowaterdata.org  --screen
nmwdi locations --pages 1 --within "NM:Socorro" --verbose --url ose.newmexicowaterdata.org  --screen --expand Things
nmwdi locations --pages 1 --within "NM:Socorro" --verbose --url ose.newmexicowaterdata.org  --screen --query "Things/properties/driller eq 'REAMY DRILLING'"
```

### MLocations
```
nmwdi mlocations --within "NM:Bernalillo" --out foo.shp 
```
