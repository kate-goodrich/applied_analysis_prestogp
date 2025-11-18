# -----------------------------------------------------------------------------
# Library setup
# -----------------------------------------------------------------------------
.libPaths("/usr/local/lib/R/site-library")

library(targets)
library(tarchetypes)
library(readxl)

# -----------------------------------------------------------------------------
# Global options for targets
# -----------------------------------------------------------------------------
tar_option_set(
    packages = c(
        "dplyr",
        "purrr",
        "sf",
        "fs",
        "arrow",
        "ggplot2",
        "lubridate",
        "mgcv",
        "rmarkdown",
        "PrestoGP",
        "readxl",
        "stringr",
        "readr"
    ),
    format = "rds",
    memory = "transient"
)

# Source helper functions
purrr::walk(list.files("R", full.names = TRUE, pattern = "\\.R$"), source)


# -----------------------------------------------------------------------------
# Targets
# -----------------------------------------------------------------------------
list(
    tarchetypes::tar_render(
        example_practice,
        "R/prestogp_example.Rmd",
        output_dir = "docs"
    ),

    tar_target(
        ref_csvs,
        build_ref_csvs(out_dir = "raw_data")
    ),
    # explicitly track the files produced
    tar_target(csn_sites_ref_file, ref_csvs$csn_sites_ref, format = "file"),
    tar_target(
        improve_sites_ref_file,
        ref_csvs$improve_sites_ref,
        format = "file"
    ),
    tar_target(params_ref_file, ref_csvs$params_ref, format = "file"),

    tar_target(
        spec_parquet_files,
        build_spec_parquet_from_refs(out_dir = "clean_data/parquet_2"),
        format = "file"
    ),

    tarchetypes::tar_render(
        exploratory_analysis,
        "R/exploratory.Rmd",
        output_dir = "docs"
    )
)
