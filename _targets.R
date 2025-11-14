# -----------------------------------------------------------------------------
# Library setup
# -----------------------------------------------------------------------------
.libPaths("/usr/local/lib/R/site-library")

library(targets)
library(tarchetypes)

# -----------------------------------------------------------------------------
# Global options for targets
# -----------------------------------------------------------------------------
tar_option_set(
    packages = c(
        "dplyr",
        "purrr",
        "sf",
        "arrow",
        "ggplot2",
        "lubridate",
        "mgcv",
        "rmarkdown",
        "PrestoGP"
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
        "R/given_example.Rmd",
        output_dir = "results"
    )
)
