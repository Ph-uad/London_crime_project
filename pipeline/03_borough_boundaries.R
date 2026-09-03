# =============================================================
# 03_borough_boundaries.R : the 33 London borough polygons as GeoJSON.
# Implements plan issue 1.10 (was 1.6).
#
# WE DO NOT SIMPLIFY. The issue proposes rmapshaper, but simplification is
# only safe if it is topology-preserving: `sf::st_simplify` moves each
# borough's boundary independently, so two neighbours that shared an edge no
# longer do, and the choropleth gets hairline slivers between them that
# cannot be repaired downstream.
#
# Instead we take a generalisation level ONS already produced across the whole
# UK coverage : where adjacent districts still share their edges exactly : and
# reduce file size by two lossless-in-practice means:
#
#   1. keep only the 33 London features and two attributes
#   2. round coordinates to BOUNDARY_COORD_DP decimal places (~0.1 m at
#      London's latitude, far finer than a 20 m generalisation)
#
# If that still misses the budget the answer is to switch BOUNDARY_GEN to
# "BUC" in _config.R and re-download : ONS's coarser product, still
# topologically sound : not to simplify here. The script says so and stops.
#
# The GSS codes are checked against boroughs.json, which is what makes a
# vintage difference between the boundary release and the LAD22 lookup
# harmless: if all 33 codes match exactly, the vintages agree for London.
#
# Writes data/processed/london.geojson
# =============================================================

source(file.path(if (dir.exists("pipeline")) "pipeline" else ".", "_common.R"))

if (!requireNamespace("sf", quietly = TRUE)) {
  fail("package 'sf' is required for boundary handling.\n",
       "       install.packages(\"sf\")   # or: apt-get install r-cran-sf")
}
suppressPackageStartupMessages(library(sf))
suppressPackageStartupMessages(library(jsonlite))

banner("03_borough_boundaries")

check(file.exists(BOUNDARY_RAW),
      "boundary file not found at '", BOUNDARY_RAW,
      "'. Run 00_download_metrics.R first.")
check(file.exists(LOOKUP_OUT), "run 00_LSAOlookup.R first.")
boroughs <- unique(fread(LOOKUP_OUT, colClasses = "character",
                         showProgress = FALSE)[, .(borough_gss, borough_name)])
setorder(boroughs, borough_gss)

message("Reading ", BOUNDARY_RAW, " (", BOUNDARY_GEN, ", ",
        BOUNDARY_ITEMS[[BOUNDARY_GEN]]$vintage, ") ...")
g <- st_read(BOUNDARY_RAW, quiet = TRUE)

# ---- Attributes ----------------------------------------------------------
code_col <- grep("^LAD\\d{2}CD$", names(g), value = TRUE)[1]
name_col <- grep("^LAD\\d{2}NM$", names(g), value = TRUE)[1]
check(!is.na(code_col) && !is.na(name_col),
      "no LADnnCD / LADnnNM columns in the boundary file. Found: ",
      paste(names(g), collapse = ", "))
message("Attributes: ", code_col, ", ", name_col, " (",
        nrow(g), " features UK-wide)")

# ---- London subset -------------------------------------------------------
# Filter before anything else: validity and reprojection only matter for the
# 33 features we ship.
code_all <- g[[code_col]]
lon <- g[grepl(LONDON_GSS_PREFIX, code_all), c(code_col, name_col)]
names(lon)[1:2] <- c("borough_gss", "borough_name")

check(nrow(lon) == LONDON_BOROUGH_N,
      "found ", nrow(lon), " London features, expected ", LONDON_BOROUGH_N,
      ". Wrong boundary product, or the vintage predates the current codes.")
check(!anyDuplicated(lon$borough_gss), "duplicate GSS code among the features.")

missing <- setdiff(boroughs$borough_gss, lon$borough_gss)
extra <- setdiff(lon$borough_gss, boroughs$borough_gss)
check(!length(missing) && !length(extra),
      "GSS codes do not match the pipeline's boroughs.\n",
      "       In the lookup but not the boundaries: ",
      paste(missing, collapse = ", "), "\n",
      "       In the boundaries but not the lookup: ",
      paste(extra, collapse = ", "),
      "\n       The boundary vintage and the LAD22 lookup disagree for London.")
ok("all ", LONDON_BOROUGH_N, " GSS codes match the pipeline's boroughs exactly")

# Use the lookup's names so map labels and data agree character for character
# : boundary files sometimes carry different casing or a suffix.
lon$borough_name <- boroughs$borough_name[match(lon$borough_gss,
                                                boroughs$borough_gss)]
lon <- lon[order(lon$borough_name), ]

check(all(st_geometry_type(lon) %in% c("POLYGON", "MULTIPOLYGON")),
      "unexpected geometry type: ",
      paste(unique(as.character(st_geometry_type(lon))), collapse = ", "))
check(!any(st_is_empty(lon)), "empty geometry for: ",
      paste(lon$borough_name[st_is_empty(lon)], collapse = ", "))

# ---- Geometry validity, in the NATIVE projected CRS ----------------------
# This must happen before reprojecting to WGS84. Once coordinates are lon/lat,
# sf validates with s2 (spherical), which is far stricter than the planar
# question we actually care about : "does this ring cross itself on the map".
# s2 rejects a merely duplicated vertex, so every ONS borough would be
# reported invalid and st_make_valid() would then damage them. Planar GEOS
# validity in the source projection is the right check.
prev_s2 <- sf::sf_use_s2()
suppressMessages(sf::sf_use_s2(FALSE))
on.exit(suppressMessages(sf::sf_use_s2(prev_s2)), add = TRUE)

valid <- st_is_valid(lon)
if (any(!valid | is.na(valid))) {
  bad <- lon$borough_name[!valid | is.na(valid)]
  message("Invalid geometry in ", length(bad), " feature(s): ",
          paste(head(bad, 5), collapse = ", "),
          if (length(bad) > 5) ", ..." else "",
          " : repairing with st_make_valid()")
  lon <- st_make_valid(lon)
  check(all(st_is_valid(lon)),
        "geometry still invalid after st_make_valid(): ",
        paste(lon$borough_name[!st_is_valid(lon)], collapse = ", "))
  ok("repaired; all ", nrow(lon), " geometries now valid")
} else {
  ok("all ", nrow(lon), " geometries valid as published (no self-intersections)")
}

# ---- Coordinate reference system ----------------------------------------
# ONS publishes in British National Grid; MapLibre needs WGS84.
crs <- st_crs(lon)
if (is.na(crs)) {
  message("No CRS declared : assuming EPSG:4326 per the GeoJSON spec.")
  st_crs(lon) <- 4326
} else if (!isTRUE(crs$epsg == 4326)) {
  from <- if (is.null(crs$epsg) || is.na(crs$epsg)) crs$input else crs$epsg
  message("Transforming from ", from, " to EPSG:4326.")
  lon <- st_transform(lon, 4326)
}

# A file that CLAIMS 4326 while carrying BNG eastings passes every other check
# and renders in the North Sea. Greater London sits inside roughly
# -0.55..0.35 E, 51.25..51.72 N, so the magnitudes settle it.
bb <- suppressWarnings(st_bbox(lon))
check(bb[["xmin"]] > -1 && bb[["xmax"]] < 1 &&
        bb[["ymin"]] > 50.5 && bb[["ymax"]] < 52,
      "bounding box is not Greater London: ",
      paste(sprintf("%s=%.1f", names(bb), bb), collapse = ", "),
      ".\n       The coordinates are probably still in British National Grid ",
      "while the file claims EPSG:4326.")
ok(sprintf("bounding box %.3f,%.3f to %.3f,%.3f : Greater London",
           bb[["xmin"]], bb[["ymin"]], bb[["xmax"]], bb[["ymax"]]))

# ---- Write ---------------------------------------------------------------
assert_not_raw_data(dirname(BOUNDARY_OUT))
ensure_dir(dirname(BOUNDARY_OUT))
if (file.exists(BOUNDARY_OUT)) unlink(BOUNDARY_OUT)
st_write(lon, BOUNDARY_OUT, driver = "GeoJSON", quiet = TRUE,
         layer_options = c(sprintf("COORDINATE_PRECISION=%d",
                                   BOUNDARY_COORD_DP),
                           "RFC7946=YES", "WRITE_BBOX=YES"))

size <- file.size(BOUNDARY_OUT)
message("  ->  ", BOUNDARY_OUT, "  (",
        format(round(size / 1024, 1), nsmall = 1), " KB)")

check(size <= BOUNDARY_MAX_BYTES,
      "london.geojson is ", round(size / 1024), " KB, over the ",
      round(BOUNDARY_MAX_BYTES / 1024), " KB budget in issue 1.10.\n",
      "       Do NOT simplify here : that breaks shared edges between ",
      "boroughs.\n       Set BOUNDARY_GEN=\"BUC\" in pipeline/_config.R and ",
      "re-run 00_download_metrics.R to take ONS's coarser product, which is ",
      "generalised across the whole coverage and keeps its topology.")
ok(round(size / 1024), " KB, within the ", round(BOUNDARY_MAX_BYTES / 1024),
   " KB budget")

# ---- Read back and check it is what a client will get --------------------
back <- fromJSON(BOUNDARY_OUT, simplifyVector = FALSE)
check(identical(back$type, "FeatureCollection"),
      "output is not a FeatureCollection.")
check(length(back$features) == LONDON_BOROUGH_N,
      "output has ", length(back$features), " features, expected ",
      LONDON_BOROUGH_N, ".")
props <- vapply(back$features, function(f) f$properties$borough_gss,
                character(1))
check(setequal(props, boroughs$borough_gss),
      "the written GeoJSON's GSS codes do not match the pipeline's boroughs.")
ok("round-trips as a ", LONDON_BOROUGH_N, "-feature FeatureCollection")

# Cross-check against the unified export when it exists, so the map and the
# data can never disagree about which boroughs are on it.
bj <- file.path(PROC_DIR, "boroughs.json")
if (file.exists(bj)) {
  bdata <- fromJSON(bj)
  in_data <- sort(unique(bdata$boroughs$gss))
  check(setequal(in_data, boroughs$borough_gss),
        "boroughs.json and london.geojson disagree on the borough set.")
  ok("GSS codes match boroughs.json")
} else {
  message("Note: boroughs.json not present : run 20_unify_metrics.R to ",
          "cross-check the two.")
}

write_log(data.table(
  product = paste0("ONS LAD ", BOUNDARY_GEN, " ",
                   BOUNDARY_ITEMS[[BOUNDARY_GEN]]$vintage),
  detail = BOUNDARY_ITEMS[[BOUNDARY_GEN]]$detail,
  simplification_applied = "none (ONS pre-generalised; topology preserved)",
  coordinate_precision_dp = BOUNDARY_COORD_DP,
  features = nrow(lon),
  bytes = size,
  crs = "EPSG:4326 (RFC 7946)"
), "boundaries.log")
