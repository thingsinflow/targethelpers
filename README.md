
<!-- README.md is generated from README.Rmd. Please edit that file -->

# targethelpers

<!-- badges: start -->

[![CRAN
status](https://www.r-pkg.org/badges/version/targethelpers)](https://CRAN.R-project.org/package=targethelpers)
[![Codecov test
coverage](https://codecov.io/gh/thingsinflow/targethelpers/graph/badge.svg)](https://app.codecov.io/gh/thingsinflow/targethelpers)
[![R-CMD-check](https://github.com/thingsinflow/targethelpers/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/thingsinflow/targethelpers/actions/workflows/R-CMD-check.yaml)
[![Lifecycle:
experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html#experimental)
<!-- badges: end -->

A simple package that provides some extra functions which I have found
to be useful when creating `targets`/`tarchetypes` pipelines for a
rather unusual purpose: scraping ephemeral data from websites.

## Features

Currently the `targethelpers` package only implement the two functions
listed below.

More functions will probably be added later on as the need arises.

### File Management Helpers

`add_filepaths_to_df(data_df, id_col_name, path, file_prefix, extension)`

> Returns the input dataframe with a `filepath` row added.

`rows_to_files(data_df, cols_not_to_compare, id_col_name, path, file_prefix, extension)`

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

You can install the development version of `targethelpers` from
[GitHub](https://github.com/) with:

``` r
# install.packages("pak")
pak::pak("thingsinflow/targethelpers")
```

## Example

This is a basic example which shows how to use
`targethelpers::rows_to_files()` as part of an automated `targets`
pipeline that saves and maintains each row of a data frame as a separate
file that can then be monitored automatically downstream:

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
          targethelpers::rows_to_files(id_col_name = "package", extension = ".rds"),
        format = "rds"
      ),
      tar_files(
        name = input,
        command = rows_as_files$data_df_w_filepaths$file_path
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
#> ✔ data completed [644ms, 1.21 kB]
#> + rows_as_files dispatched
#> INFO [2025-09-15 21:30:06] Identify rows of new, updated and/or outdated property info + update tracked files.
#> INFO [2025-09-15 21:30:06] ids_new_or_changed_rows: 100
#> INFO [2025-09-15 21:30:06] new_or_updated_files: 100
#> INFO [2025-09-15 21:30:06] deleted_files: 0
#> ✔ rows_as_files completed [156ms, 1.91 kB]
#> + input_files dispatched
#> ✔ input_files completed [0ms, 737 B]
#> + input declared [100 branches]
#> ✔ input completed [1ms, 16.45 kB]
#> + whatever declared [100 branches]
#> ✔ whatever completed [18ms, 16.45 kB]
#> ✔ ended pipeline [1.3s, 203 completed, 0 skipped]
#> # A tibble: 100 × 2
#>    package   downloads
#>    <chr>     <chr>    
#>  1 tibble    1640635  
#>  2 rlang     1616106  
#>  3 ggplot2   1609534  
#>  4 lifecycle 1576817  
#>  5 cli       1554412  
#>  6 dplyr     1488631  
#>  7 rmarkdown 1485582  
#>  8 glue      1402521  
#>  9 vctrs     1346809  
#> 10 purrr     1340272  
#> # ℹ 90 more rows
```
