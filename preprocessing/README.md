# Pre-processing

The pre-processing involves the following steps:

* Generating the following raster data using the same tiling scheme as employed for the satellite imagery (one piece of
  data per tile): 
    * A raster digital elevation model (DEM) from either the GEO1988 Copernicus DEM or the Arctic DEM; Shapefiles
    that works as indices for the two DEMs are stored on dCache. DEM tiles need to be downloaded and re-tiled 
    according to the desired tiling scheme.
    * A land/sea scene classification using the world coastline shapefile (also stored on dCache). 
    * A glacier ID mask, constructed using the Randolph glacier inventory shapefile (also stored on dCache).
    
  These rasters can be collected as assets in a STAC catalog in which each item corresponds to a tile. 
  
* Generating the following data for each of the collected scenes:
    * A shadow-enhanced satellite image, constructed from (some of) the available bands (red, blue, green, and NIR were
    downloaded to dCache).
    * A connectivity list containing the coordinates of the points on shadow profile that can be connected as shadow 
    casters/casted. This list can be stored as a plain text file.
    
    These pieces of data can be stored in a new STAC catalog, using the same catalog structure as for the original data. 
   
* Co-registration of the shadow-enhanced images. In the simplest approach this can be done by selecting a reference 
  image (highest sun angle, lowest cloud cover) and then by generate a global (X, Y)-shift that best matches each target 
  image to the reference. The resulting coordinate shift (one per scene) could be stored in any format that suits
  tabular data (to be open e.g. as a dataframe).
  

  