
<!-- README.md is generated from README.Rmd. Please edit that file -->

# targethelpers

<!-- badges: start -->

[![CRAN
status](https://www.r-pkg.org/badges/version/targethelpers)](https://CRAN.R-project.org/package=targethelpers)
[![Codecov test
coverage](https://codecov.io/gh/thingsinflow/targethelpers/graph/badge.svg?token=GX47AQ8FVA)](https://app.codecov.io/gh/thingsinflow/targethelpers)
[![R-CMD-check](https://github.com/thingsinflow/targethelpers/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/thingsinflow/targethelpers/actions/workflows/R-CMD-check.yaml)
[![Lifecycle:
experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html#experimental)
<!-- badges: end -->

A simple package that provides some extra functions which I have found
to be useful when creating
[`targets`](https://docs.ropensci.org/targets/)/[`tarchetypes`](https://docs.ropensci.org/tarchetypes/index.html)
pipelines for a rather unusual purpose: scraping ephemeral data from
websites.

## Features

Currently the `targethelpers` package only implement the two functions
listed below.

More functions will probably be added later on as the need arises.

### File Management Helpers

`add_filepaths_to_df(data_df, id_col_name, path, file_prefix, extension)`

> Returns the input dataframe with a `filepath` row added.

`rows_as_files(data_df, cols_not_to_compare, id_col_name, path, file_prefix, extension)`

> When you scrape websites you only want to scrape and process the data,
> when it has changed. This function takes the input dataframe (with a
> `filepath` row added) and saves each row as a separate file. On the
> following runs the function: - adds new rows as files (if any). -
> updates the files of existing rows that have changed (if any). -
> deletes files whose rows are not present in the dataframe any more (if
> any). The output of the function is a summary of the above changes (if
> any) as well as a list of the files currently present. The output file
> list is typically used as input for a regular `tarchetypes::tar_files`
> file monitoring pipeline step.

## Installation

The latest stable version of the `targethelpers` package can be obtained
from [CRAN](https://CRAN.R-project.org/package=targethelpers) with the
command

``` r
install.packages("targethelpers")
```

You can install the development version of the `targethelpers` package
from [GitHub](https://github.com/) with:

``` r
# install.packages("pak")
pak::pak("thingsinflow/targethelpers")
```

## Example

This is a basic example which shows how to use
`targethelpers::rows_as_files()` as part of an automated `targets`
pipeline that saves and maintains each row of a data frame as a separate
file that can then be monitored automatically further down the pipeline:

``` r
library(targethelpers)

# Typical use case as part of a targets pipeline
targets::tar_dir({
  targets::tar_script({

    library(targets)
    library(tarchetypes)

    list(
      tar_target(
        name = data,
        command = jsonlite::fromJSON("https://cranlogs.r-pkg.org/top/last-month/100")$downloads
      ),
      tar_target(
        name = rows_as_files,
        command = data |>
          # Convert each row to a file and delete files for rows that is no longer present
          targethelpers::rows_as_files(id_col_name = "package", extension = ".rds"),
        format = "rds"
      ),
      # Monitor the input files using a regular tarchetypes::tar_files() step
      tar_files(
        name = input,
        command = rows_as_files$data_w_file_paths$file_path
      ),
      tar_target(
        name = whatever,
        command = input |>
            readRDS()
        # Your code here
        ,
        pattern = map(input)
      )
    )
  })
  targets::tar_make()
  targets::tar_read(whatever)
})
#> + data dispatched
#> ✔ data completed [796ms, 1.21 kB]
#> + rows_as_files dispatched
#> INFO [2025-09-21 20:38:17] Identify rows of new, updated and/or outdated property info + update tracked files.
#> INFO [2025-09-21 20:38:17] ids_new_or_changed_rows: 100
#> INFO [2025-09-21 20:38:17] new_or_updated_files: 100
#> INFO [2025-09-21 20:38:17] deleted_files: 0
#> ✔ rows_as_files completed [159ms, 1.91 kB]
#> + input_files dispatched
#> ✔ input_files completed [0ms, 738 B]
#> + input declared [100 branches]
#> ✔ input completed [2ms, 16.47 kB]
#> + whatever declared [100 branches]
#> ✔ whatever completed [10ms, 16.47 kB]
#> ✔ ended pipeline [1.5s, 203 completed, 0 skipped]
#> # A tibble: 100 × 2
#>    package   downloads
#>    <chr>     <chr>    
#>  1 ggplot2   1751785  
#>  2 rlang     1736679  
#>  3 tibble    1718138  
#>  4 lifecycle 1676552  
#>  5 cli       1674480  
#>  6 dplyr     1604971  
#>  7 rmarkdown 1546505  
#>  8 glue      1504486  
#>  9 purrr     1484518  
#> 10 magrittr  1479089  
#> # ℹ 90 more rows
```
