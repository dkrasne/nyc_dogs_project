#install.packages(c("mapview","sf","geojson","geojsonsf"))
library(mapview)
library(sf)
library(geojson)
library(geojsonsf)

shp <- read_sf("C:/workspace/nyc_dogs_project/modzcta_shapefiles/geo_export_eaca3b68-1b4e-4677-a78b-bf52824eaf46.shp")
mapview(shp)

geojson_shp <- as.geojson(shp)
geojson_out <- geojsonsf::geojson_sf(geojson_shp)
mapview(geojson_out)

st_write(geojson_out, "C:/workspace/nyc_dogs_project/modzcta_r.geojson", driver="GeoJSON")
